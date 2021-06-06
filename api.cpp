#include <string>
#include <map>
#include <thread>

#include <cpprest/http_listener.h>
#include <nlohmann/json.hpp>
#include "api.h"
#include "database.h"
#include "CRUD/crud.h"

void HttpHandler::get(web::http::http_request& request)
{
	request.reply(web::http::status_codes::OK, "Hello World");
}

void HttpHandler::post(web::http::http_request& request)
{	
	std::wstring requestedPath = request.relative_uri().to_string().substr(0, 7);
	if (!apiRoutes["POST"].count(requestedPath)) {
		// Path does not exist
		request.reply(web::http::status_codes::NotFound, 
			nlohmann::json(
				{ 
					{"status", "failed"},
					{"msg", "Could not find Ressource you are looking for!"},
					{"path", std::string(requestedPath.begin(), requestedPath.end())}
				}
			).dump());
		return;
	}
		
	// Parse POST-Body, Fire right CRUD-Function and reply response
	nlohmann::json postData = nlohmann::json::parse(request.extract_string().get());
	nlohmann::json response = apiRoutes["POST"][requestedPath](postData);
	request.reply(web::http::status_codes::OK, response.dump());
}

void defineRoutes() {
	apiRoutes = {};

	// POST Routes
	apiRoutes["POST"][L"/create"] = CRUD::create;
	apiRoutes["POST"][L"/select"] = CRUD::select;
	apiRoutes["POST"][L"/update"] = CRUD::update;
	apiRoutes["POST"][L"/remove"] = CRUD::remove;
}

void startAPI(const int apiPort, const std::string apiAddress)
{
	defineRoutes();

	web::uri_builder uri;
		uri.set_scheme(U("http"));
		uri.set_host(utility::conversions::to_string_t(apiAddress));
		uri.set_port(utility::conversions::to_string_t(std::to_string(apiPort)));
		uri.set_path(U("/"));

	web::http::experimental::listener::http_listener listener(uri.to_uri());
	listener.support(web::http::methods::GET, HttpHandler::get);
	listener.support(web::http::methods::POST, HttpHandler::post);

	listener
		.open()
		.then([&listener, &uri]() {std::cout << "[INFO] API is up " << std::endl; })
		.wait();
	

	while (true);
}
