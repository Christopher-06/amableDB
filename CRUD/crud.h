#ifndef CRUD_H
#define CRUD_H

#include <map>
#include <vector>
#include <tuple>
#include <string>
#include <nlohmann/json.hpp>

#include "../database.h"

namespace CRUD {
	void create(nlohmann::json&, nlohmann::json&);
	void select(nlohmann::json&, nlohmann::json&);
	void update(nlohmann::json&, nlohmann::json&);
	void remove(nlohmann::json&, nlohmann::json&);
}

namespace UPDATE {
	void performUpdate(const nlohmann::json& input, const nlohmann::json& update, nlohmann::json& output);

	void increaseOperator(nlohmann::json& document, nlohmann::json& update);

	inline std::map<std::string, std::function<void(nlohmann::json&, nlohmann::json&)>> Operator = {
		{"#inc", increaseOperator}
	};
}

namespace SELECT {

	class query_t {
	public:
		std::map<size_t, float> scores; // documentId, score
		float maxScore;
		nlohmann::json query, resultInfo;

		size_t limit = 1000;

		// Database Infos
		collection_t* queryCol;
		std::set<std::tuple<std::string, DbIndex::IndexType, DbIndex::Iindex_t*>> indexedKeys;


		query_t(nlohmann::json&, std::string);
		~query_t();
		void performQuery();

		void addResults(std::vector<size_t>::iterator, std::vector<size_t>::iterator, size_t factor = 1000);
		void exportResult(std::vector<std::tuple<size_t, float>>&);
	};

	void similarOperator(const nlohmann::json&, query_t*);

	inline std::map<std::string, std::function<void(const nlohmann::json&, query_t*)>> Operator = {
		{"#limit", NULL},
		{"#similar", similarOperator}
	};
}

#endif