var fs = require("fs");
var _ = require("lodash");
var sprintf = require("sprintf-js").sprintf;
var request = require([__dirname, "request"].join("/"));
var utils = require([__dirname, "utils"].join("/"));
var flat = require("flat");

module.exports = {

    "list-jobs": {
        options: {},

        callback: function(options){
            request.perform({
                url: "/tide/jobs",
                method: "GET",
                json: true
            }, function(err, response){
                if(err){
                    process.stderr.write("Error fetching tide jobs!");
                    process.exit(1);
                }
                else if(response.statusCode != 200){
                    process.stderr.write(["Received status", response.statusCode, "when fetching tide jobs!"].join(" "));
                    process.exit(1);
                }
                else{
                    console.log(sprintf("%-30s %-50s %-20s %-25s",
                        "ID",
                        "IMAGE",
                        "INSTANCES",
                        "SCHEDULE"
                    ));

                    _.each(response.body, function(job, id){
                        console.log(sprintf("%-30s %-50s %-20s %-25s",
                            id,
                            job.application.image,
                            job.instances,
                            job.schedule
                        ));
                    });
                }
            });
        }
    },

    "show-job": {
        options: {
            application: {
                position: 1,
                help: "Name of the job to show",
                metavar: "JOB",
                required: true
            }
        },

        callback: function(options){
            request.perform({
                url: ["", "tide", "jobs", options.application].join("/"),
                method: "GET",
                json: true
            }, function(err, response){
                if(err){
                    process.stderr.write("Error fetching tide job!");
                    process.exit(1);
                }
                else if(response.statusCode != 200){
                    process.stderr.write(["Received status", response.statusCode, "when fetching tide job!"].join(" "));
                    process.exit(1);
                }
                else{
                    console.log(sprintf("%-20s %-100s", "SCHEDULE", response.body.schedule));
                    console.log(sprintf("%-20s %-100s", "INSTANCES", response.body.instances));
                    console.log(sprintf("%-20s %-100s", "ENGINE", response.body.application.engine));
                    console.log(sprintf("%-20s %-100s", "IMAGE", response.body.application.image));
                    console.log(sprintf("%-20s %-100s", "COMMAND", response.body.application.command));
                    console.log(sprintf("%-20s %-100s", "CPUS", response.body.application.cpus));
                    console.log(sprintf("%-20s %-100s", "MEMORY", response.body.application.memory));
                    console.log(sprintf("%-20s %-100s", "NETWORK MODE", response.body.application.network_mode));
                    console.log(sprintf("%-20s %-100s", "CONTAINER PORT", response.body.application.container_port || ""));
                    console.log();

                    console.log(sprintf("%-20s %-50s %-50s", "ENV VARS", "NAME", "VALUE"));
                    _.each(response.body.application.env_vars, function(val, key){
                        console.log(sprintf("%-20s %-50s %-50s", "", key, val));
                    });
                    console.log();

                    console.log(sprintf("%-20s %-50s %-50s", "TAGS", "NAME", "VALUE"));
                    _.each(flat(response.body.application.tags), function(val, key){
                        console.log(sprintf("%-20s %-50s %-50s", "", key, val));
                    });
                    console.log();

                    console.log(sprintf("%-20s %-50s %-50s", "VOLUMES", "HOST PATH", "CONTAINER PATH"));
                    _.each(flat(response.body.application.volumes), function(container, host){
                        console.log(sprintf("%-20s %-50s %-50s", "", host, container));
                    });
                }
            });
        }
    },

    "create-job": {
        options: {
            application: {
                position: 1,
                help: "Name of the job to create",
                metavar: "JOB",
                required: true
            },

            engine: {
                help: "Engine used to start application",
                metavar: "ENGINE",
                choices: ["docker"],
                abbr: "x"
            },

            image: {
                help: "Application image",
                metavar: "IMAGE",
                required: true,
                abbr: "i"
            },

            "env-var": {
                list: true,
                help: "Environment variable for application",
                metavar: "ENV_VAR=VALUE",
                abbr: "e"
            },

            "network-mode": {
                help: "Application network mode",
                metavar: "NETWORK MODE",
                abbr: "n"
            },

            "container-port": {
                help: "Port application must listen on",
                metavar: "PORT",
                abbr: "p"
            },

            command: {
                help: "Application start command",
                metavar: "COMMAND",
                abbr: "s"
            },

            tag: {
                help: "Tag to add to application",
                metavar: "NAME=VALUE",
                list: true,
                abbr: "t"
            },

            volume: {
                help: "Volume to bind-mount for application",
                metavar: "HOST_PATH:CONTAINER_PATH",
                list: true,
                abbr: "b"
            },

            cpus: {
                help: "CPUs allocated to application",
                metavar: "CPUS",
                abbr: "c"
            },

            memory: {
                help: "Memory (mb) allocated to application",
                metavar: "MEMORY",
                abbr: "m"
            },

            privileged: {
                help: "Run application containers in privileged mode",
                metavar: "PRIVILEGED",
                choices: ["true", "false"]
            },

            instances: {
                help: "Number of instances to spawn when job is scheduled to run",
                metavar: "INSTANCES",
                required: true
            },

            schedule: {
                help: "Cron schedule on which job should run",
                metavar: "SCHEDULE",
                required: true
            }
        },

        callback: function(options){
            options = _.omit(options, ["0", "_"]);

            if(_.has(options, "tag")){
                options.tags = utils.parse_tags(options.tag);
                delete options.tag;
            }

            if(_.has(options, "volume")){
                options.volumes = utils.parse_volumes(options.volume);
                delete options.volume;
            }

            if(_.has(options, "env-var")){
                options.env_vars = utils.parse_tags(options["env-var"]);
                delete options["env-var"];
            }

            if(_.has(options, "network-mode")){
                options.network_mode = options["network-mode"];
                delete options["network-mode"];
            }

            if(_.has(options, "container-port")){
                options.container_port = options["container-port"];
                delete options["container-port"];
            }

            if(_.has(options, "privileged"))
                options.privileged = options.privileged == "true";

            var config = {
                id: options.application,
                schedule: options.schedule,
                instances: options.instances,
                application: {
                    tags: options.tags,
                    env_vars: options.env_vars,
                    volumes: options.volumes,
                    network_mode: options.network_mode,
                    container_port: options.container_port,
                    privileged: options.privileged,
                    cpus: options.cpus,
                    memory: options.memory,
                    image: options.image,
                    command: options.command,
                    engine: options.engine
                }
            }

            request.perform({
                url: ["", "tide", "jobs", options.application].join("/"),
                method: "POST",
                json: config
            }, function(err, response){
                if(err){
                    process.stderr.write(JSON.stringify(err));
                    process.stderr.write("Error creating tide job!");
                    process.exit(1);
                }
                else if(response.statusCode != 201){
                    process.stderr.write(["Received status", response.statusCode, "when creating tide job!"].join(" "));
                    process.exit(1);
                }
                else
                    console.log(["Successfully created tide job:", options.application].join(" "));
            });
        }
    },

    "edit-job": {
        options: {
            application: {
                position: 1,
                help: "Name of the job to edit",
                metavar: "JOB",
                required: true
            },

            engine: {
                help: "Engine used to start application",
                metavar: "ENGINE",
                choices: ["docker"],
                abbr: "x"
            },

            image: {
                help: "Application image",
                metavar: "IMAGE",
                abbr: "i"
            },

            "env-var": {
                list: true,
                help: "Environment variable for application",
                metavar: "ENV_VAR=VALUE",
                abbr: "e"
            },

            "network-mode": {
                help: "Application network mode",
                metavar: "NETWORK MODE",
                abbr: "n"
            },

            "container-port": {
                help: "Port application must listen on",
                metavar: "PORT",
                abbr: "p"
            },

            command: {
                help: "Application start command",
                metavar: "COMMAND",
                abbr: "s"
            },

            tag: {
                help: "Tag to add to application",
                metavar: "NAME=VALUE",
                list: true,
                abbr: "t"
            },

            volume: {
                help: "Volume to bind-mount for application",
                metavar: "HOST_PATH:CONTAINER_PATH",
                list: true,
                abbr: "b"
            },

            cpus: {
                help: "CPUs allocated to application",
                metavar: "CPUS",
                abbr: "c"
            },

            memory: {
                help: "Memory (mb) allocated to application",
                metavar: "MEMORY",
                abbr: "m"
            },

            privileged: {
                help: "Run application containers in privileged mode",
                metavar: "PRIVILEGED",
                choices: ["true", "false"]
            },

            instances: {
                help: "Number of instances to spawn when job is scheduled to run",
                metavar: "INSTANCES"
            },

            schedule: {
                help: "Cron schedule on which job should run",
                metavar: "SCHEDULE"
            }
        },

        callback: function(options){
            options = _.omit(options, ["0", "_"]);

            if(_.has(options, "tag")){
                options.tags = utils.parse_tags(options.tag);
                delete options.tag;
            }

            if(_.has(options, "volume")){
                options.volumes = utils.parse_volumes(options.volume);
                delete options.volume;
            }

            if(_.has(options, "env-var")){
                options.env_vars = utils.parse_tags(options["env-var"]);
                delete options["env-var"];
            }

            if(_.has(options, "network-mode")){
                options.network_mode = options["network-mode"];
                delete options["network-mode"];
            }

            if(_.has(options, "container-port")){
                options.container_port = options["container-port"];
                delete options["container-port"];
            }

            if(_.has(options, "privileged"))
                options.privileged = options.privileged == "true";

            var config = {
                schedule: options.schedule,
                instances: options.instances,
                application: {
                    tags: options.tags,
                    env_vars: options.env_vars,
                    volumes: options.volumes,
                    network_mode: options.network_mode,
                    container_port: options.container_port,
                    privileged: options.privileged,
                    cpus: options.cpus,
                    memory: options.memory,
                    image: options.image,
                    command: options.command,
                    engine: options.engine
                }
            }

            request.perform({
                url: ["", "tide", "jobs", options.application].join("/"),
                method: "PUT",
                json: config
            }, function(err, response){
                if(err){
                    process.stderr.write("Error updating tide job!");
                    process.exit(1);
                }
                else if(response.statusCode != 200){
                    process.stderr.write(["Received status", response.statusCode, "when updating tide job!"].join(" "));
                    process.exit(1);
                }
                else
                    console.log(["Successfully updated tide job:", options.application].join(" "));
            });
        }
    },

    "remove-job": {
        options: {
            application: {
                position: 1,
                help: "Name of the job to remove",
                metavar: "JOB",
                required: true
            }
        },

        callback: function(options){
            request.perform({
                url: ["", "tide", "jobs", options.application].join("/"),
                method: "DELETE"
            }, function(err, response){
                if(err){
                    process.stderr.write("Error removing tide job!");
                    process.exit(1);
                }
                else if(response.statusCode != 204){
                    process.stderr.write(["Received status", response.statusCode, "when removing tide job!"].join(" "));
                    process.exit(1);
                }
                else{
                    console.log(["Successfully removed tide job:", options.application].join(" "));
                }
            });
        }
    },

}
