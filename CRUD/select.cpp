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
#include "../main.h"
#include "../hashing/sha256.h"

void CRUD::select(nlohmann::json& request, nlohmann::json& response) {
	try {
		// Check: projection
		std::map<std::string, bool> projection = {}; //fieldname, shouldResponded? | when .size == 0 then all fields should Responded
		if (request.contains("projection")) {
			if (!request["projection"].is_object())
				throw new nlohmann::json({ {"WrongType", "projection has to be an object"}, {"got", request["projection"]} });

			for (const auto& item : request["projection"].items()) {
				const nlohmann::json itemValue = item.value();
				const std::string itemName = item.key();

				if (!itemValue.is_boolean() & !itemValue.is_number())
					throw new nlohmann::json({ {"WrongType", "items should be a boolean"}, {"got", {itemName, itemValue} } });

				// Convert surely to bool
				if (itemValue.is_number()) {
					const int val = itemValue.get<int>();

					// Numbers over 0 are true; 0 and negative values are false
					if (val > 0)
						projection[itemName] = true;
					else
						projection[itemName] = false;
				}
					

				if (itemValue.is_boolean())
					projection[itemName] = itemValue.get<bool>();
				
			}
		}


		// Perform Query
		SELECT::query_t query = SELECT::query_t(request["query"], request["collection"].get<std::string>());
		query.performQuery();

		// Get Results and Ids of them
		std::vector<std::tuple<size_t, float>> results;
		query.exportResult(results);

		// Make Cursor and return uuid of it
		SELECT::cursor_t* cursor = new SELECT::cursor_t(query.queryCol, results, projection);
		SELECT::Cursors[cursor->myID] = cursor;
		response = { {"status", "ok"}, {"cursor_uuid", cursor->myID}, {"count", results.size()} };
	}
	catch (nlohmann::json errorMsg) {
		std::cout << "Error raised while selecting:" << std::endl << errorMsg.dump() << std::endl;
		response = { {"status", "failed"}, {"error", errorMsg} };
	}
}


namespace SELECT {
	cursor_t::cursor_t(collection_t* queryCol, std::vector<std::tuple<size_t, float>>& ids, std::map<std::string, bool> projection, size_t batchSize, size_t timeout) {
		const auto time_since_epoch = std::chrono::system_clock::now().time_since_epoch();
		const long long milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(time_since_epoch).count();
		this->myID = sha256(std::to_string(milliseconds));
		
		this->queryCol = (queryCol);
		this->ids = (ids);
		this->batchSize = batchSize;
		this->projection = projection;

		this->createdAt = std::chrono::duration_cast<std::chrono::seconds>(time_since_epoch).count();
		this->lastInteraction = this->createdAt;
		this->timeout = timeout;

		// Start with making the first batch
		std::thread(&SELECT::cursor_t::makeBatch, this).detach();
	}

	cursor_t::~cursor_t() {
		// try to lock and then delete everything
		while (!this->batchLock.try_lock())
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
			
		this->documents.clear();
		this->ids.clear();
		this->queryCol = nullptr;

		this->batchLock.unlock();
	}

	void cursor_t::makeBatch()
	{
		this->lastInteraction = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
		
		if (!this->ids.size())
			return; // Nothing to do

		std::unique_lock<std::mutex> lockGuard(this->batchLock, std::defer_lock);
		lockGuard.lock();

		std::vector<nlohmann::json> docs;
		std::vector<size_t> vectorIds;

		// Get documents
		while (this->documents.size() < size_t(this->batchSize * 1.75) && this->ids.size() > 0) {
			// Get first element
			const auto [id, score] = this->ids[0];
			
			
			for (const auto& storage : this->queryCol->storage) {
				if (storage->savedHere(id)) {
					// Saved in this storage
					docs = {};
					vectorIds = {id};

					// Get document
					storage->getDocuments(&vectorIds, docs, false, projection);
					if (docs.size()) {
						// If it found something
						this->documents.push_back({ currentDocIndex, score, docs[0] });
						++currentDocIndex;
					}
								
					break;
				}
			}

			// Erase here, because it is now inside documents
			this->ids.erase(this->ids.begin());
		}

		lockGuard.unlock();
	}

	bool cursor_t::retrieveBatch(std::vector<nlohmann::json>* documents) {
		while (this->documents.size() < this->batchSize && this->ids.size() > 0)
			std::this_thread::sleep_for(std::chrono::nanoseconds(500)); // Wait because something is in work

		// Get amount
		size_t endIndex = 0;
		if (this->documents.size() < this->batchSize || this->documents.size() == this->batchSize)
			endIndex = this->documents.size();
		else if (this->documents.size() > this->batchSize)
			endIndex = this->batchSize;
		
		// Set documents (when some are available)
		if (this->documents.size()) {
			for (size_t i = 0; i < endIndex; i++)
				documents->push_back(this->documents[i]);
			this->documents.erase(this->documents.begin(), this->documents.begin() + endIndex);
		}	

		// Make new batches
		std::thread(&cursor_t::makeBatch, this).detach();

		// Return true, when no documents and no ids left (<== When it finished)...
		return (this->documents.size() == 0 && this->ids.size() == 0);
	}

	void cursor_t::killCursor(cursor_t* cursor)
	{
		SELECT::Cursors.erase(cursor->myID);
		delete cursor;
	}

	void cursor_t::removeLeftCursors() {
		const size_t nowTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

		// Find timed out cursors
		std::vector<cursor_t*> killOrder;
		for (const auto& pair : Cursors) {
			const auto diff = nowTime - pair.second->lastInteraction;

			if (diff >= pair.second->timeout) // Timeout reached or over
				killOrder.push_back(pair.second);
		}

		// Kill them (if possible)
		for (const auto& cursor : killOrder)
			cursor_t::killCursor(cursor);

		if (Cursors.size() == 0 && killOrder.size())
			Cursors.clear(); // Clear to deallocate memory completely
	}


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
		bool allSelected = true;
		for (const auto& item : this->query.items()) {
			const nlohmann::json queryValue = item.value();
			const std::string queryName = item.key();

			if (queryName[0] == '#') {
				// Select-Operator
				if (!Operator.count(queryName))
					throw nlohmann::json({ {"OperatorError", "Given operator does not exist"}, {"Input", queryName} });

				// Fire Operator-Function
				auto func = Operator[queryName];
				if (func != NULL) {
					func(queryValue, this);
					allSelected = false;
				}
			}
			else {
				// Key-Value Search
				this->maxScore += 1000;
				allSelected = false;

				for (const auto& [keyName, type, ptr] : indexedKeys) {
					if (keyName != queryName)
						continue; // Not the key

					std::vector<size_t> results;
					if(type == DbIndex::IndexType::KeyValueIndex) {
						// Is KeyValue Index
						std::shared_ptr<DbIndex::KeyValueIndex_t>  index = std::static_pointer_cast<DbIndex::KeyValueIndex_t>(ptr);
						std::vector<std::string> indexData = { queryValue.dump() };
						results = index->perform(indexData)[0];					
					}
					else if (type == DbIndex::IndexType::MultipleKeyValueIndex) {
						// Is MultipleValue Index
						std::shared_ptr < DbIndex::MultipleKeyValueIndex_t> index = std::static_pointer_cast<DbIndex::MultipleKeyValueIndex_t>(ptr);
						std::map<std::string, std::vector<std::string>> indexMap = {};
						indexMap[queryName] = std::vector<std::string>({ queryValue.dump() });
						results = index->perform(indexMap);
					}

					if (results.size()) // Add findings
						this->addResults(results.begin(), results.end());
				}
			}
		}
	
		if (allSelected) {
			// Add all Docs
			std::vector<size_t> idContainer;
			for (const auto& storage : this->queryCol->storage)
				storage->getAllIds(idContainer);

			this->addResults(idContainer.begin(), idContainer.end());
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
	///		k, size_t (optional)
	/// </summary>
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
		std::shared_ptr<DbIndex::KnnIndex_t> knnIndex = nullptr;
		for (const auto& [keyName, type, ptr] : queryObject->indexedKeys) {
			if (type == DbIndex::IndexType::KnnIndex && keyName == fieldName) {
				// Correct index
				knnIndex = std::static_pointer_cast<DbIndex::KnnIndex_t>(ptr);
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

	/// <summary>
	/// Construct:
	///		fieldName, std::string
	///		lower, float
	///		higher, float
	/// </summary>
	void rangeOperator(const nlohmann::json& query, query_t* queryObject) {
		// Exceptions
		if (!query.contains("fieldName") | !query.contains("lower") | !query.contains("higher"))
			throw nlohmann::json({ {"KeyMissing", "fieldName/lower/higher is missing"} });
		if (!query["fieldName"].is_string())
			throw nlohmann::json({ {"WrongType", "fieldName has to be a string!"}, {"Input", query["fieldName"].type_name()} });
		if (!query["lower"].is_number() | !query["higher"].is_number())
			throw nlohmann::json({ {"WrongType", "lower and higher should to be floating points!"}, {"Input", {query["lower"].type_name(),  query["higher"].type_name()}} });

		// Parse Query
		const std::string fieldName = query["fieldName"].get<std::string>();
		float lowerBound = query["lower"].get<float>();
		float higherBound = query["higher"].get<float>();
		
		// Get Index to fieldname
		std::shared_ptr<DbIndex::RangeIndex_t> rangeIndex = nullptr;
		for (const auto& [keyName, type, ptr] : queryObject->indexedKeys) {
			if (type == DbIndex::IndexType::RangeIndex && keyName == fieldName) {
				// Correct index
				rangeIndex = std::static_pointer_cast<DbIndex::RangeIndex_t>(ptr);
				break;
			}
		}

		if (rangeIndex == nullptr)
			throw nlohmann::json({ {"IndexMissing", "No RangeIndex contains the given fieldName"}, {"Input", fieldName} });

		// Add results
		std::vector<size_t> results;
		rangeIndex->perform(lowerBound, higherBound, results);
		queryObject->addResults(results.begin(), results.end());
		queryObject->maxScore += 1000;
	}
}

