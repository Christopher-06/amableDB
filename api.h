#ifndef API_H
#define API_H

#include <string>
#include <cpprest/http_listener.h>

inline std::thread apiRunner;
inline std::map<std::string, std::map<std::wstring, std::function<nlohmann::json(nlohmann::json)>>> apiRoutes;

namespace HttpHandler {
	void get(web::http::http_request&);
	void post(web::http::http_request&);
}

void defineRoutes();

void startAPI(const int apiPort, const std::string apiAddress);

#endif