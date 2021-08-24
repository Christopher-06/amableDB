#include <string>
#include <map>
#include <thread>
#include <future>
#include <chrono>

#include <boost/algorithm/string/replace.hpp>
#include "Simple-Web-Server/server_http.hpp"
#include "Simple-Web-Server/client_http.hpp"
#include <nlohmann/json.hpp>
#include "api.h"
#include "main.h"
#include "database.h"
#include "CRUD/crud.h"

using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;
using HttpClient = SimpleWeb::Client<SimpleWeb::HTTP>;

namespace Endpoints {
	namespace GET {
		void index(std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request) {
			API::writeJSON(response, { {"status", "ok"} });
		}

		void cursor(std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request)
		{
			auto query_params = API::parseQueryString(request);


			if (!query_params.count("cursor_uuid")) {
				API::writeJSON(response, { {"status", "failed"}, {"KeyMissing", "cursor_uuid is missing"} });
				return;
			}

			if (!SELECT::Cursors.count(query_params["cursor_uuid"])) {
				API::writeJSON(response, { {"status", "failed"}, {"CannotFind", "No cursor is listed with this uuid"}, {"input", query_params["cursor_uuid"]} });
				return;
			}

			// Get Cursor
			SELECT::cursor_t* cursor = SELECT::Cursors[query_params["cursor_uuid"]];
			std::vector<nlohmann::json> documents = {};

			// get all? Yes ==> set batch_size to ids.size() and make batch
			if (query_params.count("all") && TrueWritings.count(query_params["all"])) {
				cursor->batchSize = std::numeric_limits<size_t>::max();
				cursor->makeBatch();
			}

			// Retrieve Batch
			bool hasFinished = cursor->retrieveBatch(&documents);
			if (hasFinished)
				std::thread(SELECT::cursor_t::killCursor, cursor).detach();

			API::writeJSON(response, { {"status", "ok"}, {"count", documents.size()}, {"items", documents}, {"finished", hasFinished} });
		}	
	}

	namespace POST {
		void indexes(std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request)
		{
			// Parse Body
			nlohmann::json postBody;
			if (!API::parseBody(request, postBody)) {
				API::writeJSON(response, postBody, "400 Bad Request");
				return;
			}
			// Parse Query Parameter
			std::map<std::string, std::string> queryParameter = API::parseQueryString(request);


			std::set<std::string> rebuildIndexes = {};

			// Iterate through Objects
			for (const auto& item : postBody.items()) {
				const std::string indexName = item.key();
				const nlohmann::json indexOp = item.value();
				const std::string colName = indexOp["collection"].get<std::string>();

				if (!collections.count(colName)) {// Collection does not exist
					API::writeJSON(response, {
						{"status", "failed"},
						{"msg", "Collection does not exist"},
						{"collection", colName}
					});
					return;
				}

				// Define index
				if (indexOp.contains("definition") && indexOp["definition"].is_object()) {
					if (collections[colName]->indexes.count(indexName)) {
						// Index is already defined
						API::writeJSON(response, {
							{"status", "ok"},
							{"msg", "Index does already exists"},
							{"index", indexName}
						});
						return;
					}

					nlohmann::json definition = indexOp["definition"];
					definition["name"] = indexName;

					// Create Index, set and directly build it
					std::shared_ptr<DbIndex::Iindex_t> index = DbIndex::loadIndexFromJSON(definition);
					collections[colName]->indexes[indexName] = index;
					rebuildIndexes.insert(colName);
				}
			}

			// Succed everything
			API::writeJSON(response, { {"status", "ok"} });

			// Rebuild Indexes
			if (!queryParameter.count("bulk") && !TrueWritings.count(queryParameter["bulk"])) {
				for (const std::string& colName : rebuildIndexes)
					std::thread(&collection_t::BuildIndexes, collections[colName]).detach();

				//if(rebuildIndexes.size()) // Indicates wheter it changed something
				//	std::thread(saveDatabase, DATA_PATH).detach();
			}
			
		}

		void bulk(std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request)
		{
			// Parse Body
			nlohmann::json postBody;
			if (!API::parseBody(request, postBody)) {
				API::writeJSON(response, postBody, "400 Bad Request");
				return;
			}

			auto bulkFunc = [](const std::string route, const std::string val, std::shared_ptr<nlohmann::json> result) {
				try {
					HttpClient client(API_ADDRESS + ":" + std::to_string(API_PORT));
					auto request = client.request("POST", route, val);
					*result = nlohmann::json::parse(request->content.string());
				}
				catch (const SimpleWeb::system_error& e) {
					*result = nlohmann::json({ {"status", "failed"}, {"ConnectionError", e.what()} });
				}
			};

			// Post-Processing
			std::set<std::string> rebuildIndexes = {}; // Name of collections

			// Iterate through bulk-items
			std::vector<std::shared_ptr<nlohmann::json>> results = {};
			std::vector<std::thread> tasks = {};
			for (const auto& item : postBody) {
				// Get Parameter
				std::string route = item["method"];
				std::string val = item["value"].dump();

				// Set post-processing
				{
					// Index Rebuildings
					if (route.find("/create") != std::string::npos) {
						// Extract collections
						for (const auto& newDocs : item["value"].items()) {
							if(collections.count(newDocs.key()))
								rebuildIndexes.insert(newDocs.key());
						}
					}
						
					if (route.find("/index") != std::string::npos) {
						// Extract collections
						for (const auto& definition : item["value"]) {
							if(definition.contains("collection") && collections.count(definition["collection"].get<std::string>()))
								rebuildIndexes.insert(definition["collection"].get<std::string>());
						}

					}

				}

				// Set bulk-info
				{
					// Add (maybe) ?-sign
					if (route.find('?') == std::string::npos)
						route += "?";

					// Add (maybe) &-sign
					if(route[route.length() - 1] != '&')
						route += "&";

					// Add bulk-info
					route += "bulk=true";
				}

				// Prepare Result
				nlohmann::json clientResult = { {"status", "undefined"} };
				auto resultPtr = std::make_shared<nlohmann::json>(clientResult);
				results.push_back(resultPtr);

				// Fire Client
				std::thread t(bulkFunc, route, val, resultPtr);
				tasks.push_back(std::move(t));
			}

			// Get bulk-items results and combine them into one object
			nlohmann::json responseBody = { {"status", "ok"}, {"bulk", {}} };
			size_t counter = 0;
			for (const auto& clientResult : results) {
				// Wait, get Result and increment counter
				tasks[counter].join(); 
				responseBody["bulk"][counter] = *clientResult;
				++counter;
			}

			// Response
			API::writeJSON(response, responseBody);
		
			// Do Post-Processing
			for (const std::string& colName : rebuildIndexes)
				std::thread(&collection_t::BuildIndexes, collections[colName]).detach();
		}
			
		std::function<void(std::shared_ptr<HttpServer::Response>, std::shared_ptr<HttpServer::Request>)> crud_wrapper(std::function<void(nlohmann::json&, nlohmann::json&)> func, const std::string operation) {
			return [func, operation](std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request) {
				// Parse Body
				nlohmann::json postBody;
				if (!API::parseBody(request, postBody)){
					API::writeJSON(response, postBody, "400 Bad Request");
					return;
				}

				// Parse Query Parameter
				std::map<std::string, std::string> queryParameter = API::parseQueryString(request);

				// Perform CRUD
				nlohmann::json crudResult = {};
				func(postBody, crudResult);

				// Response
				API::writeJSON(response, crudResult);

				// Post Processing
				if (operation == "create") {
					// Rebuild Indexes
					{
						std::set<std::string> rebuildIndexes = {};
						if (!queryParameter.count("bulk") && !TrueWritings.count(queryParameter["bulk"])) {
							// Extract collections
							for (const auto& elements : postBody.items()) {
								const std::string colName = elements.key();
								if (collections.count(colName))
									rebuildIndexes.insert(colName);
							}
						}

						for (const std::string colName : rebuildIndexes)
							std::thread(&collection_t::BuildIndexes, collections[colName]).detach();
					}
				}
			};
		}
	}


	std::function<void(std::shared_ptr<HttpServer::Response>, std::shared_ptr<HttpServer::Request>)> Preprocessing(
		std::function<void(std::shared_ptr<HttpServer::Response>, std::shared_ptr<HttpServer::Request>)> func,
		bool inThread ) {

		// Return the Wrapped Function
		return [func, inThread](std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request) {
			// Possible Pre-Processing

			// Define func (Route) in Thread
			std::thread task([func, response, request]() {
				try {
					// Fire Function
					func(response, request);
				}
				catch (std::exception ex) {
					// Log Error in console
					std::cout << "Error on handling Client: " << std::endl;
					std::cout << "	- Method: " << request->method << std::endl;
					std::cout << "	- Body: " << request->content.string() << std::endl;
					std::cout << "	- Path: " << request->path << std::endl;
					std::cout << "	- Remote Endpoint: " << request->remote_endpoint().address() << " - " << request->remote_endpoint().port() << std::endl;
					std::cout << "	- " << ex.what() << std::endl;

					// Send failed response
					API::writeJSON(response, { {"status", "failed"}, {"InternalServerError", ex.what()} }, "500 Internal Server Error");
				}
				

				// Possible Post-Processing...
			});

			if (inThread)
				task.detach();
			else
				task.join();
		};
	}
}

namespace API {
	
	inline void writeJSON(std::shared_ptr<HttpServer::Response> response, const nlohmann::json json, const std::string status_code)
	{
		std::string content = json.dump();
		*response << "HTTP/1.1 " << status_code << "\r\n"
			<< "Content-Length: " << content.length() << "\r\n"
			<< "Content-Type: application/json" << "\r\n"
			<< "\r\n" << content;

	}

	inline std::map<std::string, std::string> parseQueryString(const std::shared_ptr<HttpServer::Request> request)
	{
		std::map<std::string, std::string> query_params = {};
		for (auto& field : request->parse_query_string())
			query_params[field.first] = field.second;

		return query_params;
	}

	inline bool parseBody(const std::shared_ptr<HttpServer::Request> request, nlohmann::json& body)
	{
		try {
			// Try parsing
			const std::string content = request->content.string();
			body = nlohmann::json::parse(content);
			return true;
		}
		catch (nlohmann::detail::parse_error ex) {
			// JSON Parse error
			body = { {"status", "failed"}, {"JsonParseError", ex.what()} };
		}

		return false;
	}

	inline void defineRoutes() {
		//	GET METHODS
		server.resource["^/$"]["GET"] = Endpoints::Preprocessing(Endpoints::GET::index);
		server.resource["^/cursor$"]["GET"] = Endpoints::Preprocessing(Endpoints::GET::cursor);


		// POST METHODS
		server.resource["^/index$"]["POST"] = Endpoints::Preprocessing(Endpoints::POST::indexes);
		server.resource["^/bulk$"]["POST"] = Endpoints::Preprocessing(Endpoints::POST::bulk);

		//	- CRUD
		server.resource["^/create$"]["POST"] = Endpoints::POST::crud_wrapper(CRUD::create, "create");
		server.resource["^/select$"]["POST"] = Endpoints::POST::crud_wrapper(CRUD::select, "select");
		server.resource["^/update$"]["POST"] = Endpoints::POST::crud_wrapper(CRUD::update, "update");
		server.resource["^/remove$"]["POST"] = Endpoints::POST::crud_wrapper(CRUD::remove, "remove");
	}

	void startAPI(const int apiPort, const std::string apiAddress)
	{
		server.config.port = apiPort;
		server.config.address = apiAddress;

		defineRoutes();

		serverThread = std::thread([]() {server.start(); });
		std::cout << "[INFO] API is up at: http://" << apiAddress << ":" << apiPort << std::endl;
	}
}




