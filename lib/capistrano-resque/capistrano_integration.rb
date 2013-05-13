require "capistrano"
require "capistrano/version"

module CapistranoResque
  class CapistranoIntegration
    TASKS = [
      'resque:status',
      'resque:start',
      'resque:stop',
      'resque:restart',
      'resque:shutdown',
      'scheduler:start',
      'scheduler:stop',
      'scheduler:restart'
    ]

    def self.load_into(capistrano_config)
      capistrano_config.load do
        before(CapistranoIntegration::TASKS) do
          _cset(:app_env)            { (fetch(:rails_env) rescue 'production') }
          _cset(:workers)            { [{:queue => "*", :worker_count => 1, :interval => 5}] }
          _cset(:resque_kill_signal) { "SIGQUIT" }
          _cset(:resque_env)         { nil }
          _cset(:resque_bundle)      { fetch(:bundle_cmd) rescue 'bundle' }
        end

        def remote_process_exists?(pid_file)
          "[ -e #{pid_file} ] && kill -0 `cat #{pid_file}` > /dev/null 2>&1"
        end

        def workers_roles
          roles = workers.map {|w| w[:role] }.compact
          roles  = [ :resque_worker ] if roles.empty?

          roles
        end

        def for_each_workers(&block)
          workers_roles.each do |role|
            yield(role, workers.select { |w| (w[:role] == role) || ( w[:role].nil? && role == :resque_worker ) })
          end
        end

        def resque_send_signal(signal, pid)
          "kill -s #{signal} #{pid}"
        end

        def status_command
          script = <<-END
            if [ -e #{current_path}/tmp/pids/resque_work_1.pid ]; then
              for f in $(ls #{current_path}/tmp/pids/resque_work*.pid);
                do ps -p $(cat $f) | sed -n 2p ; done
            ;fi
          END

          script
        end

        def start_command(queue, pid, interval)
          if queue.split(",").length > 1
            queues = "QUEUES=#{queue}"
          else
            queues = "QUEUE=#{queue}"
          end

          script = <<-END
            if [ -e #{pid} ]; then
              if kill -0 `cat #{pid}` > /dev/null 2>&1 ; then
                echo "Resque worker already running";
                exit 0;
              fi;

              rm #{pid};
            fi;

            cd #{current_path} && PIDFILE=#{pid} BACKGROUND=yes VERBOSE=1 INTERVAL=#{interval} #{queues} #{resque_env} #{resque_bundle} exec rake resque:work
          END

          script
        end

        def stop_command
          script = <<-END
            if [ `find #{current_path}/tmp/pids -name "resque_work*.pid" | wc -w` -gt 0 ]; then
              for f in `ls #{current_path}/tmp/pids/resque_work*.pid`; do
                #{try_sudo} kill -s #{resque_kill_signal} `cat $f` && rm $f;
              done
            fi
          END

          script
        end

        def force_stop_command
          script = <<-END
            if [ `find #{current_path}/tmp/pids -name "resque_work*.pid" | wc -w` -gt 0 ]; then
              for f in `ls #{current_path}/tmp/pids/resque_work*.pid`; do
                #{try_sudo} kill -s SIGKILL `cat $f` && rm $f;
              done
            fi
          END

          script
        end

        def start_scheduler(pid)
          "cd #{current_path} && RAILS_ENV=#{app_env} \
           PIDFILE=#{pid} BACKGROUND=yes VERBOSE=1 \
           #{fetch(:bundle_cmd, "bundle")} exec rake resque:scheduler"
        end

        def stop_scheduler(pid)
          "if [ -e #{pid} ]; then \
            #{try_sudo} kill $(cat #{pid}) ; rm #{pid} \
           ;fi"
        end

        namespace :resque do
          desc "See current worker status"
          task :status, :roles => lambda { workers_roles() }, :on_no_matching_servers => :continue do
            run(status_command)
          end

          desc "Start Resque workers"
          task :start, :roles => lambda { workers_roles() }, :on_no_matching_servers => :continue do
            for_each_workers do |role, workers|
              worker_id = 1
              workers.each do |worker|
                queue = worker[:queue]
                number_of_workers = worker[:worker_count] || 1
                interval = worker[:interval] || 1

                logger.info "Starting #{number_of_workers} worker(s) with QUEUE: #{queue}"
                threads = []
                number_of_workers.times do
                  pid = "#{current_path}/tmp/pids/resque_work_#{worker_id}.pid"
                  threads << Thread.new(pid) { |pid| run(start_command(queue, pid, interval), :roles => role) }
                  worker_id += 1
                end
                threads.each(&:join)
              end
            end
          end

          # See https://github.com/resque/resque#signals for a descriptions of signals
          # QUIT - Wait for child to finish processing then exit (graceful)
          # TERM / INT - Immediately kill child then exit (stale or stuck)
          # USR1 - Immediately kill child but don't exit (stale or stuck)
          # USR2 - Don't start to process any new jobs (pause)
          # CONT - Start to process new jobs again after a USR2 (resume)
          desc "Quit running Resque workers"
          task :stop, :roles => lambda { workers_roles() }, :on_no_matching_servers => :continue do
            run(stop_command)
          end

          desc "Restart running Resque workers"
          task :restart, :roles => lambda { workers_roles() }, :on_no_matching_servers => :continue do
            stop

            # wait at most 30s until resque stopped
            # TODO check if process is running
            sleep 30

            # force stop if not stopped yet
            run(force_stop_command)

            start
          end

          namespace :scheduler do
            desc "Starts resque scheduler with default configs"
            task :start, :roles => :resque_scheduler do
              pid = "#{current_path}/tmp/pids/scheduler.pid"
              run(start_scheduler(pid))
            end

            desc "Stops resque scheduler"
            task :stop, :roles => :resque_scheduler do
              pid = "#{current_path}/tmp/pids/scheduler.pid"
              run(stop_scheduler(pid))
            end

            task :restart do
              stop
              start
            end
          end
        end
      end
    end
  end
end

if Capistrano::Configuration.instance
  CapistranoResque::CapistranoIntegration.load_into(Capistrano::Configuration.instance)
end
