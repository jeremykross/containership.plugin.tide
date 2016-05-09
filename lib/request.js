var _ = require("lodash");
var request = require("request");

module.exports = {

    config: null,

    perform: function(options, fn){
        if(this.config["api-url"].indexOf("https://api.containership.io") == 0){
            var original_url = options.url;
            options.url = [
                "https://api.containership.io",
                "v2",
                "organizations",
                this.config.headers["x-containership-cloud-organization"],
                "clusters",
                this.config.headers["x-containership-cloud-cluster"],
                "proxy"
            ].join("/");

            var original_method = options.method;
            options.method = "POST";

            options.headers = _.pick(this.config.headers, [
                "authorization"
            ]);

            var original_qs = options.qs;
            options.qs = {};

            var original_body = options.json;

            options.json = {
                url: original_url,
                qs: original_qs,
                method: original_method
            }

            if((original_method == "POST" || original_method == "PUT") && !_.isUndefined(original_body))
                options.json.data = original_body;
        }
        else
            options.url = [this.config["api-url"], "/", this.config["api-version"], options.url].join("");

        request(options, fn);
    }

}
