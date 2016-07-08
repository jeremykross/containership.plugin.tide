var _ = require("lodash");
var ContainershipPlugin = require("containership.plugin");
var tide_scheduler = require([__dirname, "lib", "tide-scheduler"].join("/"));
var routes = require([__dirname, "lib", "routes"].join("/"));
var cli = require([__dirname, "lib", "cli"].join("/"));
var request = require([__dirname, "lib", "request"].join("/"));
var nomnom = require("nomnom");

module.exports = new ContainershipPlugin({
    type: ["core", "cli"],
    name: "tide",

    initialize: function(core){
        if(_.has(core, "logger")){
            if(core.options.mode == "leader"){
                // register tide logger
                core.logger.register("tide-scheduler");

                // initialize tide scheduler
                tide_scheduler.initialize(core);

                // initialize routes
                routes.initialize(core);
            }
        }
        else{
            request.config = this.get_config("cli");

            var commands = _.map(cli, function(configuration, command){
                configuration.name = command;
                return configuration;
            });

            return { commands: commands };
        }
    },

    reload: function(){
        server.exit();
    }
});
