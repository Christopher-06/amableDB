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

	void GET_cursor(nlohmann::json& request, nlohmann::json& response) {
		if (!request.contains("cursor_uuid")) {
			response = { {"status", "failed"}, {"KeyMissing", "cursor_uuid is missing"} };
			return;
		}

		if(!request["cursor_uuid"].is_string()) {
			response = { {"status", "failed"}, {"WrongType", "cursor_uuid should be a string!"} };
			return;
		}

		std::string cursorUUID = request["cursor_uuid"].get<std::string>();
		if (!SELECT::Cursors.count(cursorUUID)) {
			response = { {"status", "failed"}, {"CannotFind", "No cursor is listed with this uuid"}, {"input", cursorUUID} };
			return;
		}
		
		// Get Cursor
		SELECT::cursor_t* cursor = SELECT::Cursors[cursorUUID];
		std::vector<nlohmann::json> documents = {};

		// get all? Yes ==> set batch_size to ids.size() and make batch
		if (request.contains("all") && TrueWritings.count(request["all"].get<std::string>())) {
			cursor->batchSize = std::numeric_limits<size_t>::max();
			cursor->makeBatch();
		}
			
		// Retrieve Batch
		bool hasFinished = cursor->retrieveBatch(&documents);
		if (hasFinished)
			std::thread(SELECT::cursor_t::killCursor, cursor).detach();

		response = { {"status", "ok"}, {"count", documents.size()}, {"items", documents}, {"finished", hasFinished} };
	}
}

void HttpHandler::get(web::http::http_request request)
{
	// Get URI
	std::string uri = utility::conversions::to_utf8string(request.relative_uri().to_string());
	size_t splitIndex = uri.find_first_of('?');
	auto requestedUri = uri.substr(0, splitIndex);
	boost::replace_all(requestedUri, "/", "");

	// Parse Parameter
	nlohmann::json parameter;
	if(splitIndex != std::wstring::npos){
		std::string requestedParameter = uri.substr(splitIndex + 1);
		std::string currentParameter;

		// As long as something is in the string
		while (requestedParameter.size()) {
			if (requestedParameter[0] == '&') // Remove maybe first &
				requestedParameter = requestedParameter.substr(1);

			// Get one Parameter
			splitIndex = requestedParameter.find_first_of('&');
			if (splitIndex != std::wstring::npos)
				currentParameter = requestedParameter.substr(0, splitIndex);
			else
				currentParameter = requestedParameter.substr(); // Complete

			// Erase current from requests
			boost::replace_all(requestedParameter, currentParameter, "");

			// Split to extract key and value
			splitIndex = currentParameter.find_first_of('=');
			if (splitIndex == std::wstring::npos)
				continue; // Not an key value

			// Enter key-value as std::string
			std::string wkey = currentParameter.substr(0, splitIndex);
			std::string wvalue = currentParameter.substr(splitIndex + 1);
			//auto skey = std::string(wkey.begin(), wkey.end());
			//auto svalue = std::string(wvalue.begin(), wvalue.end());
			parameter[wkey] = wvalue;
		}
	}
	
	// Does the Ressource exist?
	if (!apiRoutes["GET"].count(requestedUri)) {
		request.reply(web::http::status_codes::NotFound,
			nlohmann::json(
				{
					{"status", "failed"},
					{"msg", "Could not find Ressource you are looking for!"},
					{"path", std::string(requestedUri.begin(), requestedUri.end())},
				{"parameter", parameter}
				}
		).dump(), "application/json");
		return;
	}
	
	// Fire function and reply
	nlohmann::json response;
	apiRoutes["GET"][requestedUri](parameter, response);
	request.reply(web::http::status_codes::OK, response.dump(), "application/json");
}

void HttpHandler::post(web::http::http_request request)
{	
	std::string path = utility::conversions::to_utf8string(request.relative_uri().to_string());
	std::string requestedPath = std::string(path.begin(), path.end());
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
			).dump(), "application/json");
		return;
	}
		
	// Parse POST-Body, Fire right Function and reply response
	nlohmann::json postData = nlohmann::json::parse(request.extract_string().get());
	nlohmann::json response;
	apiRoutes["POST"][requestedPath](postData, response);
	request.reply(web::http::status_codes::OK, response.dump(), "application/json");
}

void defineRoutes() {
	apiRoutes = {};

	// POST Routes
	apiRoutes["POST"]["create"] = CRUD::create;
	apiRoutes["POST"]["select"] = CRUD::select;
	apiRoutes["POST"]["update"] = CRUD::update;
	apiRoutes["POST"]["remove"] = CRUD::remove;
	apiRoutes["POST"]["index"] = Endpoints::POST_Index;
	
	// GET Routes
	apiRoutes["GET"]["cursor"] = Endpoints::GET_cursor;
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
