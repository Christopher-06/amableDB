#ifndef CRUD_H
#define CRUD_H

#include <map>
#include <vector>
#include <tuple>
#include <string>
#include <nlohmann/json.hpp>

#include "../database.h"

namespace CRUD {
	nlohmann::json create(nlohmann::json& documents);
	nlohmann::json select(nlohmann::json& documents);
	nlohmann::json update(nlohmann::json& documents);
	nlohmann::json remove(nlohmann::json& documents);
}

namespace SELECT {

	class query_t {
	public:
		std::map<size_t, float> scores; // documentId, score
		float maxScore = 0;
		nlohmann::json query, resultInfo;

		size_t limit = 1000;

		// Database Infos
		collection_t* queryCol;
		std::set<std::tuple<std::string, DbIndex::IndexType, DbIndex::Iindex_t*>> indexedKeys;


		query_t(nlohmann::json&, std::string);
		~query_t();
		void performQuery();

		void addResults(std::vector<size_t>::iterator, std::vector<size_t>::iterator, size_t factor = 1000);
		std::vector<std::tuple<size_t, float>> exportResult();
	};

	void similarOperator(const nlohmann::json&, query_t*);

	inline std::map<std::string, std::function<void(const nlohmann::json&, query_t*)>> Operator = {
		{"#limit", NULL},
		{"#similar", similarOperator}
	};
}

#endif