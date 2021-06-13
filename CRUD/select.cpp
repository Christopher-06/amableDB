#include <iostream>
#include <vector>
#include <map>

#include <algorithm> 
#include <nlohmann/json.hpp>

#include "crud.h"
#include "../database.h"


nlohmann::json CRUD::select(nlohmann::json& request) {
	try {
		SELECT::query_t query = SELECT::query_t(request["query"], request["collection"].get<std::string>());
		query.performQuery();

		// Add Findings to Response
		nlohmann::json results = { {"status", "ok"}, {"result", {}} };
		size_t index = 0;
		for (const auto& [documentID, score] : query.exportResult()) {
			results["result"].push_back({ index, { {"score", score}, {"id", documentID} } });
			++index;
		}
		results["count"] = index;

		return results;
	}
	catch (nlohmann::json errorMsg) {
		std::cout << "Error raised while selecting:" << std::endl << errorMsg.dump() << std::endl;
		return { {"status", "failed"}, {"error", errorMsg} };
	}
}


namespace SELECT {
	query_t::query_t(nlohmann::json& query, std::string collectionName)
	{
		this->query = query;
		if (query.contains("#limit") && query["#limit"].is_number())
			this->limit = query["#limit"].get<size_t>();

		if (!collections.count(collectionName)) {
			// No Collection with that name exists (Create dummy)
			this->queryCol = new collection_t("dummy");
			this->resultInfo.push_back({ {"Collection has no items"} });
		} else
			this->queryCol = collections[collectionName];		
		this->indexedKeys = this->queryCol->getIndexedKeys();		
	}

	query_t::~query_t()
	{
		// Check if it is dummy collection
		if (!collections.count(this->queryCol->name))
			delete this->queryCol;
	}

	void query_t::performQuery()
	{
		for (const auto& item : this->query.items()) {
			const nlohmann::json queryValue = item.value();
			const std::string queryName = item.key();

			if (queryName[0] == '#') {
				// Select-Operator
				if (!Operator.count(queryName))
					throw nlohmann::json({ {"OperatorError", "Given operator does not exist"}, {"Input", queryName} });

				// Fire Operator-Function
				auto func = Operator[queryName];
				if(func != NULL)
					func(queryValue, this);
			}
			else {
				// Key-Value Search
				this->maxScore += 1000;

				for (const auto& [keyName, type, ptr] : indexedKeys) {
					if (keyName != queryName)
						continue; // Not the key

					std::vector<size_t> results;
					if(type == DbIndex::IndexType::KeyValueIndex) {
						// Is KeyValue Index
						DbIndex::KeyValueIndex_t* index = dynamic_cast<DbIndex::KeyValueIndex_t*>(ptr);
						std::vector<std::string> indexData = { queryValue.dump() };
						results = index->perform(indexData)[0];					
					}
					else if (type == DbIndex::IndexType::MultipleKeyValueIndex) {
						// Is MultipleValue Index
						DbIndex::MultipleKeyValueIndex_t* index = dynamic_cast<DbIndex::MultipleKeyValueIndex_t*>(ptr);
						std::map<std::string, std::vector<std::string>> indexMap = {};
						indexMap[queryName] = std::vector<std::string>({ queryValue.dump() });
						results = index->perform(indexMap);
					}

					if (results.size()) // Add findings
						this->addResults(results.begin(), results.end());
				}
			}
		}
	}

	void query_t::addResults(std::vector<size_t>::iterator begin, std::vector<size_t>::iterator end, size_t factor)
	{
		while (begin != end) {
			this->scores[*begin] += factor;
			++begin;
		}
	}

	std::vector<std::tuple<size_t, float>> query_t::exportResult()
	{
		// Convert scores to vector of tuples
		std::vector<std::tuple<size_t, float>> result;
		result.reserve(this->scores.size());

		for (const auto& [id, score] : this->scores) {
			if(score >= this->maxScore)
				result.push_back(std::make_tuple(id, score));
		}

		// Sort by Score
		std::sort(result.begin(), result.end(), [](const std::tuple<size_t, float> a, const std::tuple<size_t, float> b) {
			return (std::get<1>(a) > std::get<1>(b));
		});

		// Slice, baddest is at the end
		if (result.size() > limit)
			result = std::vector<std::tuple<size_t, float>>(result.begin(), result.begin() + this->limit);

		// Calculate actual scores now
		for (auto it = result.begin(); it != result.end(); ++it)
			*it = std::make_pair(std::get<0>(*it), std::get<1>(*it) / this->maxScore);

		return result;
	}

	//	** Operations **

	/// <summary>
	/// Construct:
	///		fieldName, std::string
	///		value, std::vector<float>
	/// </summary>
	/// <param name="query"></param>
	void similarOperator(const nlohmann::json& query, query_t* queryObject) {
		// Exceptions
		if (!query.contains("fieldName") | !query.contains("value")) 
			throw nlohmann::json({ {"KeyMissing", "fieldName or value is missing"}});		
		if(!query["fieldName"].is_string())
			throw nlohmann::json({ {"WrongType", "fieldName has to be a string!"}, {"Input", query["fieldName"].type_name()}});
		if(!query["value"].is_array())
			throw nlohmann::json({ {"WrongType", "value has to be a vector/array of floats!"}, {"Input", query["value"].type_name()} });

		const std::string fieldName = query["fieldName"].get<std::string>();
		const std::vector<float> value = query["value"].get<std::vector<float>>();

		if(value.size() == 0)
			throw nlohmann::json({ {"ZeroItems", "value vector's lenght is 0"} });

		// Get Index to fieldname
		DbIndex::KnnIndex_t* knnIndex = nullptr;
		for (const auto& [keyName, type, ptr] : queryObject->indexedKeys) {
			if (type == DbIndex::IndexType::KnnIndex && keyName == fieldName) {
				// Correct index
				knnIndex = dynamic_cast<DbIndex::KnnIndex_t*>(ptr);
				break;
			}
		}

		if(knnIndex == nullptr)
			throw nlohmann::json({ {"IndexMissing", "No knn-Index contains the given fieldName"}, {"Input", fieldName}});

		// Finally Perform Index. results[0] has the farest distance
		size_t totalDocs = queryObject->queryCol->countDocuments();
		std::priority_queue<std::pair<float, hnswlib::labeltype>> results = knnIndex->perform(value, totalDocs);

		if (results.size()) {		
			float maxDistance = -1;

			while(!results.empty()) {
				// Get item
				auto [currentDistance, id] = results.top();
				results.pop();

				if (maxDistance < 0) 
					maxDistance = std::ceil(currentDistance + 1);
				

				queryObject->scores[id] += maxDistance - currentDistance;
			}		
		}
	}
}

