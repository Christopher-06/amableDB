#include <iostream>
#include <vector>
#include <map>
#include <thread>
#include <chrono>
#include <mutex>

#include <algorithm> 
#include <nlohmann/json.hpp>

#include "crud.h"
#include "../database.h"


void CRUD::select(nlohmann::json& request, nlohmann::json& response) {
	try {
		// Perform Query
		SELECT::query_t query = SELECT::query_t(request["query"], request["collection"].get<std::string>());
		query.performQuery();

		// Get Results and Ids of them
		std::vector<std::tuple<size_t, float>> results;
		query.exportResult(results);

		std::vector<size_t> resultIds;
		for (const auto& [documentID, score] : results)
			resultIds.push_back(documentID);

		// Get documents
		std::map<size_t, nlohmann::json> documents;
		std::mutex mut;
		auto func = [&documents, &resultIds, &mut](group_storage_t* storage) {
			// Get from Storage
			std::vector<nlohmann::json> docs;
			storage->getDocuments(&resultIds, docs);

			// Insert in locked map
			mut.lock();
			for (const auto& doc : docs) {
				size_t id = doc["id"].get<size_t>();
				documents[id] = doc;
			}
			mut.unlock();
		};

		// Set workers for all Storages and wait
		std::vector<std::thread> workers;
		for (auto& storage : query.queryCol->storage)
			workers.push_back(std::thread(func, storage));
		for (size_t i = 0; i < workers.size(); ++i)
		{
			if (workers[i].joinable())
				workers[i].join();
		}

		// Combine documents and Indexes
		size_t index = 0;
		response = { {"status", "ok"}, {"result", {}} };
		for (const auto& [documentID, score] : results) {
			if (documents.count(documentID)) {
				response["result"].push_back({ index, score, documents[documentID] });
				++index;
			}		
		}

		response["count"] = index;
	}
	catch (nlohmann::json errorMsg) {
		std::cout << "Error raised while selecting:" << std::endl << errorMsg.dump() << std::endl;
		response = { {"status", "failed"}, {"error", errorMsg} };
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
					if (ptr->isInWork)
						continue; // Cannot use it

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

	void query_t::exportResult(std::vector<std::tuple<size_t, float>>& result)
	{
		// Convert scores to vector of tuples
		result.reserve(this->scores.size());
		for (const auto& [id, score] : this->scores) {
			if(score >= this->maxScore)
				result.push_back(std::make_tuple(id, score));
		}

		// Sort by Score
		std::sort(result.begin(), result.end(), [](const std::tuple<size_t, float> a, const std::tuple<size_t, float> b) {
			return (std::get<1>(a) > std::get<1>(b));
		});

		// Slice, the baddest One is at the end
		if (result.size() > limit)
			result = std::vector<std::tuple<size_t, float>>(result.begin(), result.begin() + this->limit);

		if (this->maxScore < 1)
			this->maxScore = 1;

		// Calculate actual scores now
		for (auto it = result.begin(); it != result.end(); ++it)
			*it = std::make_pair(std::get<0>(*it), std::get<1>(*it) / this->maxScore);
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
		
		if (value.size() == 0)
			throw nlohmann::json({ {"ZeroItems", "value vector's lenght is 0"} });

		// Get kValue
		size_t kValue = 0;
		if (query.contains("k") && query["k"].is_number_integer())
			kValue = query["k"].get<size_t>();
		else // Set to default all Documents
			kValue = queryObject->queryCol->countDocuments();	

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
		std::priority_queue<std::pair<float, hnswlib::labeltype>> results;
		knnIndex->perform(value, &results, kValue);

		// Because the keyValue Selection adds always 1.000 to the maxScore and we do not 
		// want that this function can overwhelm the keyValue Selection, we only add
		// max. 850 to the score:
		// 
		//				 	maxDistance - curDistance
		//	item Score  +=	-------------------------
		//						maxDistance / 850		<=== denominator
		//

		if (results.size()) {		
			float maxDistance = -1, denominator = -1;

			while(!results.empty()) {
				// Get item
				auto [currentDistance, id] = results.top();
				results.pop();

				if (maxDistance < 0) { 
					// First One has the farest distance
					maxDistance = currentDistance + 1;
					denominator = maxDistance / 850;
				}
				

				queryObject->scores[id] += (maxDistance - currentDistance) / denominator;
			}		
		}
	}
}

