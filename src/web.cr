require "armature"
require "armature/form"

class Conveyor::Web
  include HTTP::Handler
  include Armature::Route

  getter path : String

  def initialize(@path = "/conveyor")
  end

  def call(context, render_app_shell = true)
    route context do |r, response, session|
      response.headers["content-type"] = "text/html"

      render "header" if render_app_shell

      r.root do
        r.get do
          job_keys = [] of String
          queue_keys = [] of String
          belt_keys = [] of String
          redis.scan_each match: "conveyor:*", count: 1000 do |key|
            pp key: key
            case key
            when .starts_with? "conveyor:job:"
              job_keys << key
            when .starts_with? "conveyor:queue:"
              queue_keys << key
            when .starts_with? "conveyor:belt:"
              belt_keys << key
            end
          end

          queue_entries = redis.pipeline do |pipe|
            queue_keys.each do |key|
              pipe.lrange key, "0", "-1"
            end
          end
          queues = queue_keys
            .map_with_index do |key, index|
              {key.lchop("conveyor:queue:"), queue_entries[index].as(Array)}
            end
            .to_h
          scheduled_job_ids = redis.zrange("conveyor:scheduled", "0", "-1").as(Array)
          belt_ids = belt_keys.map(&.lchop("conveyor:belt:")).to_set

          all_jobs = redis
            .pipeline do |pipe|
              job_keys.each do |key|
                pipe.hgetall(key)
              end
            end
            .compact_map do |data|
              data = data.as(Array)
              unless data.empty?
                JobData.new Redis.to_hash(data)
              end
            end
            .to_set

          jobs_by_queue = all_jobs.group_by(&.queue)

          enqueued_jobs = all_jobs
            .select { |job| queues[job.queue]?.try &.includes? job.id }
            .to_set
          scheduled_jobs = all_jobs
            .select { |job| scheduled_job_ids.includes? job.id }
            .to_set
          pending_jobs = all_jobs
            .select { |job| job.pending? && belt_ids.includes?(job.belt) }
            .to_set
          orphaned_jobs = all_jobs - enqueued_jobs - scheduled_jobs - pending_jobs

          pp(
            all_jobs: all_jobs.map(&.id).sort,
            enqueued: enqueued_jobs.map(&.id).sort,
            scheduled: scheduled_jobs.map(&.id).sort,
            pending: pending_jobs.map(&.id).sort,
            orphaned: orphaned_jobs.map(&.id).sort,
          )

          render "app"
        end
      end

      r.on "jobs" do
        r.on id: UUID do |id|
          r.delete do
            redis.unlink "conveyor:job:#{id}"
            response.redirect @path
          end
        end
      end

      render "footer"
    end
  end

  macro render(template, to io = nil)
    Armature::Template.embed "lib/conveyor/web/templates/{{template.id}}.ecr", {{io || "response".id}}
  end

  delegate redis, to: CONFIG

  abstract struct Component
    macro def_to_s(template)
      def to_s(io) : Nil
        Conveyor::Web.render {{template}}, to: io
      end
    end
  end

  record JobsTable(T) < Component, base_path : String, jobs : Enumerable(JobData), session : T do
    def_to_s "jobs_table"
  end

  struct Form(T) < Component
    getter method : String
    getter action : String
    getter session : T
    @block : ->

    def initialize(@method, @action, @session, &@block)
    end

    def_to_s "form"
  end
end
