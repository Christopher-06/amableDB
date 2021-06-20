#include <string>
#include <map>
#include <thread>

#include <boost/algorithm/string/replace.hpp>
#include <cpprest/http_listener.h>
#include <nlohmann/json.hpp>
#include "api.h"
#include "main.h"
#include "database.h"
#include "CRUD/crud.h"

namespace Endpoints {
	void POST_Index(nlohmann::json& indexData, nlohmann::json& response) {
		// Iterate through Objects
		for (const auto& item : indexData.items()) {
			const std::string indexName = item.key();
			const nlohmann::json indexOp = item.value();
			const std::string colName = indexOp["collection"].get<std::string>();

			if (!collections.count(colName)) {// Collection does not exist
				response = { 
					{"status", "failed"}, 
					{"msg", "Collection does not exist"}, 
					{"collection", colName} 
				};
				return;
			}

			// Define index
			if (indexOp.contains("definition") && indexOp["definition"].is_object()) {
				if (collections[colName]->indexes.count(indexName)) {
					// Index is already defined
					response = { 
						{"status", "ok"}, 
						{"msg", "Index does already exists"}, 
						{"index", indexName} 
					};
					return;
				}

				nlohmann::json definition = indexOp["definition"];
				definition["name"] = indexName;

				// Create Index, set and directly build it
				DbIndex::Iindex_t* index = DbIndex::loadIndexFromJSON(definition);
				collections[colName]->indexes[indexName] = index;
				index->buildIt(collections[colName]->storage);
				std::thread(saveDatabase, DATA_PATH).detach();
			}

			// Build Index
			if (indexOp.contains("build") && indexOp["build"].get<bool>()) {
				if (!collections[colName]->indexes.count(indexName)) {
					// Index is never defined
					response = { 
						{"status", "failed"}, 
						{"msg", "Index does not exist. Please create one first"}, 
						{"index", indexName} 
					};
					return;
				}

				collections[colName]->indexes[indexName]->buildIt(collections[colName]->storage);
			}
		}

		response = { {"status", "ok"} };
	}
}

void HttpHandler::get(web::http::http_request request)
{
	request.reply(web::http::status_codes::OK, "Hello World");
}

void HttpHandler::post(web::http::http_request request)
{	
	auto path = request.relative_uri().to_string();
	std::wstring requestedPath = std::wstring(path.begin(), path.end());
	boost::replace_all(requestedPath, "/", "");

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
		
	// Parse POST-Body, Fire right Function and reply response
	nlohmann::json postData = nlohmann::json::parse(request.extract_string().get());
	nlohmann::json response;
	apiRoutes["POST"][requestedPath](postData, response);
	request.reply(web::http::status_codes::OK, response.dump());
}

void defineRoutes() {
	apiRoutes = {};

	// POST Routes
	apiRoutes["POST"][L"create"] = CRUD::create;
	apiRoutes["POST"][L"select"] = CRUD::select;
	apiRoutes["POST"][L"update"] = CRUD::update;
	apiRoutes["POST"][L"remove"] = CRUD::remove;
	apiRoutes["POST"][L"index"] = Endpoints::POST_Index;
	
}

void startAPI(const int apiPort, const std::string apiAddress)
{
	defineRoutes();
	
	uri.set_scheme(U("http"));
	uri.set_host(utility::conversions::to_string_t(apiAddress));
	uri.set_port(utility::conversions::to_string_t(std::to_string(apiPort)));
	uri.set_path(U("/"));

	listener = web::http::experimental::listener::http_listener(uri.to_uri());
	listener.support(web::http::methods::GET, HttpHandler::get);
	listener.support(web::http::methods::POST, HttpHandler::post);

	listener.open()
		.then([]() {std::cout << "[INFO] API is up at: " << uri.to_string().c_str() << std::endl; })
		.wait();
}
