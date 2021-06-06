#ifndef CRUD_H
#define CRUD_H

#include <nlohmann/json.hpp>

namespace CRUD {
	nlohmann::json create(nlohmann::json& documents);
	nlohmann::json select(nlohmann::json& documents);
	nlohmann::json update(nlohmann::json& documents);
	nlohmann::json remove(nlohmann::json& documents);
}



#endif