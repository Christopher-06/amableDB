#ifndef API_H
#define API_H

#include <string>
#include <thread>
#include <map>
#include <cpprest/http_listener.h>
#include <nlohmann/json.hpp>

inline web::uri_builder uri;
inline web::http::experimental::listener::http_listener listener;
inline std::thread apiRunner;
inline std::map<std::string, std::map<std::wstring, std::function<nlohmann::json(nlohmann::json&)>>> apiRoutes;

namespace Endpoints {
	nlohmann::json POST_Index(nlohmann::json&);
}

namespace HttpHandler {
	void get(web::http::http_request);
	void post(web::http::http_request);
}

void defineRoutes();

void startAPI(const int apiPort, const std::string apiAddress);

#endif