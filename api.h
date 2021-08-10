#ifndef API_H
#define API_H

#include <string>
#include <thread>
#include <map>
#include "Simple-Web-Server/server_http.hpp"

#include <nlohmann/json.hpp>

using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;


namespace API {
	inline HttpServer server;
	inline std::thread serverThread;

	inline void writeJSON(std::shared_ptr<HttpServer::Response> response, const nlohmann::json json, const std::string status_code = "200 OK");
	
	inline std::map<std::string, std::string> parseQueryString(const std::shared_ptr<HttpServer::Request> request);
	inline bool parseBody(const std::shared_ptr<HttpServer::Request> request, nlohmann::json& body);

	inline void defineRoutes();

	void startAPI(const int apiPort, const std::string apiAddress);
}

namespace Endpoints {
	namespace GET {
		void index(std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request);

		void cursor(std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request);
		void collection(std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request);
	}

	namespace POST {
		void indexes(std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request);
		void bulk(std::shared_ptr<HttpServer::Response> response, std::shared_ptr<HttpServer::Request> request);

		std::function<void(std::shared_ptr<HttpServer::Response>, std::shared_ptr<HttpServer::Request>)> crud_wrapper(std::function<void(nlohmann::json&, nlohmann::json&)> func, const std::string operation);
	}

	void POST_Index(nlohmann::json&, nlohmann::json&);

	void GET_cursor(nlohmann::json&, nlohmann::json&);
	void GET_collection(nlohmann::json&, nlohmann::json&);

	std::function<void(std::shared_ptr<HttpServer::Response>, std::shared_ptr<HttpServer::Request>)> Preprocessing(
		std::function<void(std::shared_ptr<HttpServer::Response>, std::shared_ptr<HttpServer::Request>)> func,
		bool inThread = true
	);
}



#endif