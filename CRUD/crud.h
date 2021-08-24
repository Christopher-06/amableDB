#ifndef CRUD_H
#define CRUD_H

#include <map>
#include <vector>
#include <tuple>
#include <string>
#include <thread>
#include <chrono>

#include <nlohmann/json.hpp>
#include "../database.h"
#include "../main.h"

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

	class cursor_t {
	private:
		std::vector<std::tuple<size_t, float>> ids;
		nlohmann::json documents;
		size_t currentDocIndex = 0;
		collection_t* queryCol;
		std::map<std::string, bool> projection;
		
		size_t createdAt, lastInteraction, timeout; // All in seconds | timeout = 30 Min
				
		std::mutex batchLock;		

	public:
		std::string myID;
		size_t batchSize;

		cursor_t(collection_t* queryCol, std::vector<std::tuple<size_t, float>>& ids, std::map<std::string, bool> projection, size_t batchSize = 50, size_t timeout = 1800);
		~cursor_t();

		void makeBatch();
		bool retrieveBatch(std::vector<nlohmann::json>* documents);

		static void killCursor(cursor_t* cursor);
		static void removeLeftCursors();
	};

	class query_t {
	public:
		std::map<size_t, float> scores; // documentId, score
		float maxScore = 0;
		nlohmann::json query, resultInfo;

		size_t limit = 1000;

		// Database Infos
		collection_t* queryCol;
		std::set<std::tuple<std::string, DbIndex::IndexType, std::shared_ptr<DbIndex::Iindex_t>>> indexedKeys;


		query_t(nlohmann::json&, std::string);
		~query_t();
		void performQuery();

		void addResults(std::vector<size_t>::iterator, std::vector<size_t>::iterator, size_t factor = 1000);
		void exportResult(std::vector<std::tuple<size_t, float>>&);
	};

	void similarOperator(const nlohmann::json&, query_t*);
	void rangeOperator(const nlohmann::json&, query_t*);

	inline std::map<std::string, std::function<void(const nlohmann::json&, query_t*)>> Operator = {
		{"#limit", NULL},
		{"#similar", similarOperator},
		{"#range", rangeOperator}
	};

	inline std::map<std::string, cursor_t*> Cursors = {}; // uuid, cursor
}

#endif