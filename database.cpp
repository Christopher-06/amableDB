#include <iostream>
#include <fstream>
#include <filesystem>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <chrono>
#include <thread>
#include <random>

#include <nlohmann/json.hpp>
#include "hnswlib.h"

#include "sha256.h"
#include "main.h"
#include "database.h"
#include "storage.h"


//	*** LOADING ***
void loadCollection(std::string collectionPath) {
	// Firstly load metadata-file (.metadata extension). If it does not exist, we cannot 
	// load the collection ==> just return and cout message
	std::mutex coutLock;
	if (!std::filesystem::exists(collectionPath + "/collection.metadata")) {
		std::cout << "Attention: Something went wrong! Collection may be corrupted" << std::endl;
		std::cout << "Collection cannot be loaded: " << collectionPath << " Aborted." << std::endl;
		return;
	}

	// Load and Parse metadata
	std::ifstream fs(collectionPath + "/collection.metadata", std::fstream::in);
	nlohmann::json metadata;
	metadata << fs;
	fs.close();

	// Create collection and set metadata
	collection_t* col = new collection_t(metadata["name"]);
	collections[metadata["name"]] = col;

	// Set indexes
	if (metadata.contains("indexes") && metadata["indexes"].is_array()) {
		for (const auto& indexMeta : metadata["indexes"]) {
			// Parse JSON Object to Index
			const std::string indexName = indexMeta["name"].get<std::string>();
			const nlohmann::json indexObject = nlohmann::json(indexMeta);
			DbIndex::Iindex_t* index = DbIndex::loadIndexFromJSON(indexObject);

			// Set Index in collection
			if (index != nullptr)
				col->indexes[indexName] = index;
			else
				std::cout << "Cannot load index: " << indexName << std::endl;
		}
	}

	// Iterate through the collectionPath-Folder and load all storage files (.knndb extension)
	std::vector<std::thread> workers;
	for (auto& file : std::filesystem::directory_iterator(collectionPath)) {
		if (file.path().filename().u8string().find(".knndb") == std::string::npos)
			continue; // No storage file knndb file

		// Load .KNNDB File in another Thread 
		workers.push_back(std::thread([file, col, &coutLock]() {
			try {
				group_storage_t* storage = new group_storage_t(file.path().u8string());
				col->storage.push_back(storage);
			}
			catch (std::exception ex) {
				// File is corrupted
				// ==> happens when new created documents are inserted and the program exits when it never 
				//		wrote down or when it is writing down and the program exits. Then the other file 
				//		should yet exist and we can savely remove this file (new created docs are unlucky/maybe later we fix that (: )
				std::filesystem::remove(file);
			}
		}));
	}

	// Finish all Worker
	for (size_t i = 0; i < workers.size(); ++i)
	{
		if (workers[i].joinable())
			workers[i].join();
	}

	// Build indexes in the background
	std::thread(&collection_t::BuildIndexes, col).detach();
}

void loadDatabase(std::string dataPath) {
	// Iterate through the dataPath-Folder and check all folders for collection-folders
	std::cout << "[INFO] Loading Collections..." << std::endl;

	for (const auto& file : std::filesystem::directory_iterator(dataPath)) {
		const std::string filePath = file.path().u8string();
		const std::string fileName = file.path().filename().u8string();

		if (fileName.find("col_") != std::string::npos && std::filesystem::is_directory(filePath)) {
			// Found saved collection
			std::cout << "	- " << fileName;
			loadCollection(filePath);
			std::cout << "	==> SUCCESS" << std::endl;
		}
	}

	std::cout << "[INFO] Loaded all Collections. Indexes are building in the background" << std::endl;
}

void saveDatabase(std::string dataPath) {
	// Save Collections
	for (const auto& col : collections) {
		std::unique_lock<std::mutex> lockGuard(col.second->saveLock, std::defer_lock);
		for (const auto storage : col.second->storage)
			storage->save();

		lockGuard.lock();

		// Save Metadata to JSON-Object		
		nlohmann::json dbMetadata;
		dbMetadata["name"] = col.first;
		dbMetadata["indexes"] = DbIndex::saveIndexesToString(col.second->indexes);

		// Write JSON down
		std::ofstream metadataFile (dataPath + "/col_" + col.first + "/collection.metadata", std::fstream::trunc);
		metadataFile << dbMetadata.dump();
		metadataFile.close();

		lockGuard.unlock();
	}
}


//	*** collection_t ***
collection_t::collection_t(std::string name)
{
	this->storage = {};
	this->name = name;
}

std::vector<size_t> collection_t::insertDocuments(const std::vector<nlohmann::json> documents)
{
	if (!documents.size())
		return {}; // Emtpy

	std::random_device rndDev;
	std::mt19937 rng(rndDev());

	// Find one random storage container
	group_storage_t* storage = nullptr;
	size_t storageSize = this->storage.size();
	for (size_t i = 0; i < storageSize; ++i) {
		storage = this->storage[std::rand() % storageSize];

		if(storage->countDocuments() < MAX_ELEMENTS_IN_STORAGE)
			break; // is not full
	}

	// Check if Full, all containers are full or less than 10 containers exist
	if (storage == nullptr || storage->countDocuments() >= MAX_ELEMENTS_IN_STORAGE || this->storage.size() < 10) {
		// Create new Storage File
		const std::string storagePath = DATA_PATH + "/col_" + this->name + "/storageNew" + std::to_string(rng()) + ".knndb";
		storage = new group_storage_t(storagePath);
		this->storage.push_back(storage);
	}

	// Enter all Documents
	std::vector<size_t> entereredIds(documents.size());
	auto enteredIdIT = entereredIds.begin();
	for (auto doc : documents) {
		if (!doc.contains("id")) {
			// Create unused ID
			bool hasUnusedOne = false;
			while (!hasUnusedOne) {
				size_t value = static_cast<size_t>(rng());
				doc["id"] = value;

				// Check if it is used
				hasUnusedOne = true;
				for (const auto& item : this->storage) {
					if (item->savedHere(value)) {
						hasUnusedOne = false;
						break;
					}
				}
			}	
		}

		storage->insertDocument(doc);
		*enteredIdIT = doc["id"];
		++enteredIdIT;
	}
	return entereredIds;
}

std::set<std::tuple<std::string, DbIndex::IndexType, DbIndex::Iindex_t*>> collection_t::getIndexedKeys()
{
	std::set<std::tuple<std::string, DbIndex::IndexType, DbIndex::Iindex_t*>> list;

	// Get all Keys from all Indexes
	for (const auto& index : this->indexes) {
		const auto type = index.second->getType();

		for(const auto& keyName : index.second->getIncludedKeys())
			list.insert(std::tuple<std::string, DbIndex::IndexType, DbIndex::Iindex_t*>(keyName, type, index.second));
	}
		
	return list;
}

void collection_t::BuildIndexes()
{
	if (!this->indexes.size())
		return; // No Indexes exists

	// Mark Waiting Stage: If try_lock (or own_lock) returns false, someone else is already waiting
	// so we can just return
	std::unique_lock<std::mutex> waitLock(this->indexBuilderWaiting, std::try_to_lock);
	if (!waitLock.owns_lock())
		return;

	// Mark Working Stage: Waiting until we can start and then mark that we
	// are not waiting anymore
	std::unique_lock<std::mutex> workLock(this->indexBuilderWorking);
	waitLock.unlock();

	// Create the Indexes completly new (get Savestring and reload them) in the background and reset them directly
	std::map<std::string, DbIndex::Iindex_t*> backgroundIndexes = {};
	for (const auto& [name, index] : this->indexes) {
		std::map<std::string, DbIndex::Iindex_t*> m = { { name, index } };
		auto metadata = DbIndex::saveIndexesToString(m)[0];
		backgroundIndexes[name] = DbIndex::loadIndexFromJSON(metadata);
		backgroundIndexes[name]->reset();
	}

	auto workerFunc = [&backgroundIndexes](const nlohmann::json& item) {
		// Add Item to all Indexes
		for (const auto& [name, index] : backgroundIndexes)
			index->addItem(item);
	};

	// Iterate through storages and process all items (in Threads)
	std::vector<std::thread> storageWorker = {};
	for (const auto& storage : this->storage)
		storageWorker.push_back(std::thread(&group_storage_t::doFuncOnAllDocuments, storage, workerFunc));

	// Wait for all to complete
	for (auto& worker : storageWorker)
		worker.join();

	// Finish all Indexes and unlock
	for (const auto& [name, index] : backgroundIndexes) {
		index->finish();
		index->inBuilding = false;
	}

	// Swap Background and Old indexes
	std::map<std::string, DbIndex::Iindex_t*> oldIndexes = this->indexes;
	for (auto& [name, index] : backgroundIndexes)
		this->indexes[name] = index;

	// Delete old Indexes in 15 sec
	std::thread([](std::map<std::string, DbIndex::Iindex_t*> oldIndexes) {
		std::this_thread::sleep_for(std::chrono::seconds(15));
		for (auto& [name, index] : oldIndexes) {
			delete oldIndexes[name];
			oldIndexes[name] = nullptr;
			
		}		
	}, oldIndexes).detach();
}

size_t collection_t::countDocuments()
{
	size_t total = 0;

	for (const auto& storage : this->storage)
		total += storage->countDocuments();

	return total;
}


//	*** Indexes ***

//	*** KeyValue Index ***
DbIndex::KeyValueIndex_t::KeyValueIndex_t(std::string keyName, bool isHashedIndex)
{
	this->perfomedOnKey = keyName;
	this->isHashedIndex = isHashedIndex;
	this->data = {};
	this->createdAt = 0;
}

void DbIndex::KeyValueIndex_t::reset()
{
	std::unique_lock<std::mutex> lockGuard(this->useLock);
	this->data.clear();
}

void DbIndex::KeyValueIndex_t::finish()
{
	// Set time
	this->createdAt = std::chrono::duration_cast<std::chrono::seconds>(
		std::chrono::system_clock::now().time_since_epoch() // Since 1970
		).count();
}

void DbIndex::KeyValueIndex_t::addItem(const nlohmann::json& item)
{
	if (!item.contains(this->perfomedOnKey))
		return;

	// document contains the key
	auto dataKey = item[this->perfomedOnKey];
	if (this->isHashedIndex)
		dataKey = sha256(dataKey.dump());

	std::unique_lock<std::mutex> lockGuard(this->useLock);
	this->data[dataKey].push_back(item["id"].get<size_t>());
}

std::vector<std::vector<size_t>> DbIndex::KeyValueIndex_t::perform(std::vector<std::string>& values)
{
	while (this->inBuilding)
		std::this_thread::sleep_for(std::chrono::nanoseconds(25));
	std::unique_lock<std::mutex> lockGuard(this->useLock);

	std::vector<std::vector<size_t>> result = {};
	if (!this->data.size() || !values.size()) // Nothing available: Not build or No data
		return result;

	// Find for every Value the result and push it to result
	result.reserve(values.size());
	for (const std::string& value : values) {
		if (this->isHashedIndex) {
			result.push_back(this->data[sha256(value)]);
		}
		else {
			auto obj = nlohmann::json::parse(value);
			result.push_back(this->data[obj]);
		}
	}

	return result;
}

nlohmann::json DbIndex::KeyValueIndex_t::saveMetadata()
{
	nlohmann::json metadata;

	metadata["type"] = DbIndex::IndexType::KeyValueIndex;
	metadata["isHashedIndex"] = this->isHashedIndex;
	metadata["keyName"] = this->perfomedOnKey;

	return metadata;
}

std::set<std::string> DbIndex::KeyValueIndex_t::getIncludedKeys()
{
	return { this->perfomedOnKey };
}


//	*** Multiple KeyValue Index ***
DbIndex::MultipleKeyValueIndex_t::MultipleKeyValueIndex_t(std::vector<std::string> keyNames, bool isFullHashedIndex, std::vector<bool> isHashedIndex)
{
	this->indexes = {};
	this->keyNames = keyNames;
	this->isFullHashedIndex = isFullHashedIndex;
	this->isHashedIndex = isHashedIndex;

	// Enter all indexes	
	for (int i = 0; i < keyNames.size(); ++i) {
		this->indexes.push_back(new DbIndex::KeyValueIndex_t(
			keyNames[i], 
			(isFullHashedIndex || (isHashedIndex.size() == keyNames.size() && isHashedIndex[i]))
		));
	}

	this->createdAt = 0;
}

void DbIndex::MultipleKeyValueIndex_t::reset()
{
	for (const auto& subIndex : this->indexes)
		subIndex->reset();
}

void DbIndex::MultipleKeyValueIndex_t::finish()
{
	// Set time
	this->createdAt = std::chrono::duration_cast<std::chrono::seconds>(
		std::chrono::system_clock::now().time_since_epoch() // Since 1970
		).count();
}

void DbIndex::MultipleKeyValueIndex_t::addItem(const nlohmann::json& item)
{
	for (const auto& subIndex : this->indexes)
		subIndex->addItem(item);
}

std::vector<size_t> DbIndex::MultipleKeyValueIndex_t::perform(std::map<std::string, std::vector<std::string>>& query) {
	while (this->inBuilding)
		std::this_thread::sleep_for(std::chrono::nanoseconds(25));
	std::unique_lock<std::mutex> lockGuard(this->useLock);
	
	if (!this->indexes.size() || !query.size()) // Nothing available: Not build or No data
		return std::vector<size_t>();

	// Get all Findings from the Sub Indexes and calculate all ids
	std::map<size_t, size_t> idScores = {}; //1. doc id 2. score
	size_t maxScore = 0;
	for (const auto& index : this->indexes) {
		if (query.count(index->perfomedOnKey)) { 
			// Query-key is indexes here
			for (const auto& subResult : index->perform(query[index->perfomedOnKey])) {
				// Perform Index 
				for (const auto& docID : subResult) {
					// Add 1 to every result id
					if (idScores.count(docID))
						idScores[docID] += 1;
					else
						idScores[docID] = 1;
				}
			}	
			++maxScore;
		}		
	}

	// Get all ids, which got the maxScore
	std::set<size_t> result = {};
	for (const auto& pair : idScores) {
		if (pair.second == maxScore) // maxScore is achieved		
			result.insert(pair.first);
		
	}

	return std::vector<size_t>(result.begin(), result.end());
}

nlohmann::json DbIndex::MultipleKeyValueIndex_t::saveMetadata()
{
	nlohmann::json metadata;

	metadata["type"] = DbIndex::IndexType::MultipleKeyValueIndex;
	metadata["keyNames"] = this->keyNames;
	metadata["isFullHashedIndex"] = this->isFullHashedIndex;
	metadata["isHashedIndex"] = this->isHashedIndex;

	return metadata;
}

std::set<std::string> DbIndex::MultipleKeyValueIndex_t::getIncludedKeys()
{
	std::set<std::string> keys = {};

	for (const auto& subIndex : this->indexes)
		keys.insert(subIndex->perfomedOnKey);

	return keys;
}


//	*** Knn Index ***
DbIndex::KnnIndex_t::KnnIndex_t(std::string keyName, size_t space)
{
	this->perfomedOnKey = keyName;
	this->space = hnswlib::L2Space(space);
	this->index = new hnswlib::HierarchicalNSW<float>(&this->space, 0);;
	this->spaceValue = space;
	this->createdAt = 0;
	this->elementCount = 0;
}

std::vector<std::vector<size_t>> DbIndex::KnnIndex_t::perform(std::vector<std::vector<float>>& query, size_t limit)
{
	std::vector<std::vector<size_t>> results;
	results.reserve(query.size());

	std::vector<size_t> queryR (limit);
	for (const auto& value : query) {
		// Calculate K-Nearest-Neighbours
		auto gd = this->index->searchKnn(value.data(), limit);
		int gdIndex = gd.size() - 1;

		// Reverse and remove Distance at the same time
		while (!gd.empty()) {
			const auto [rDis, rID] = gd.top();
			queryR[gdIndex] = rID;

			gd.pop();
			--gdIndex;
		}

		results.push_back(queryR);
	}

	return results;
}

void DbIndex::KnnIndex_t::reset()
{
	std::unique_lock<std::mutex> lockGuard(this->useLock);
	delete this->index;

	// Default 7 elements
	this->index = new hnswlib::HierarchicalNSW<float>(&(this->space), 7);
}

void DbIndex::KnnIndex_t::finish()
{
	// Set time
	this->createdAt = std::chrono::duration_cast<std::chrono::seconds>(
		std::chrono::system_clock::now().time_since_epoch() // Since 1970
		).count();
}

void DbIndex::KnnIndex_t::addItem(const nlohmann::json& item)
{
	const auto document = item;
	if (!document.contains(this->perfomedOnKey))
		return;
	if (!document[this->perfomedOnKey].is_array())
		return;

	// Got the key and it is an array
	std::vector<float> data = std::vector<float>(this->spaceValue);
	auto it = data.begin();

	// Build data-vector with only floats
	for (const auto& item : document[this->perfomedOnKey].items()) {
		const auto value = item.value();
		if (value.is_number_float() || value.is_number() || value.is_number_integer())
			*it = value.get<float>(); // Is integral value
		else if (value.is_string())
			*it = std::stof(value.get<std::string>()); // Was string
		else
			*it = 0; // Something weird is that

		++it;
		if (it == data.end())
			break; // There are too much, just abort
	}

	// Add Padding
	while (it != data.end()) {
		*it = 0;
		++it;
	}

	std::unique_lock<std::mutex> lockGuard(this->useLock);

	// Add one more to index
	this->elementCount += 1;
	dynamic_cast<hnswlib::HierarchicalNSW<float>*>(this->index)->resizeIndex(this->elementCount);

	// Add Datapoint when it is locked
	hnswlib::labeltype _id = document["id"].get<size_t>();
	this->index->addPoint(data.data(), _id);
}

void DbIndex::KnnIndex_t::perform(const std::vector<float>& query, std::priority_queue<std::pair<float, hnswlib::labeltype>>* result, size_t limit) {
	while (this->inBuilding)
		std::this_thread::sleep_for(std::chrono::nanoseconds(25));
	std::unique_lock<std::mutex> lockGuard(this->useLock);

	*result = this->index->searchKnn(query.data(), limit);
}

nlohmann::json DbIndex::KnnIndex_t::saveMetadata()
{
	nlohmann::json metadata;

	metadata["type"] = DbIndex::IndexType::KnnIndex;
	metadata["keyName"] = this->perfomedOnKey;
	metadata["space"] = this->spaceValue;

	return metadata;
}

std::set<std::string> DbIndex::KnnIndex_t::getIncludedKeys()
{
	return { this->perfomedOnKey };
}


//	*** Range Index ***
DbIndex::RangeIndex_t::RangeIndex_t(std::string keyName)
{
	this->perfomedOnKey = keyName;
	this->data = {};
	this->createdAt = 0;
}

void DbIndex::RangeIndex_t::reset()
{
	std::unique_lock<std::mutex> lockGuard(this->useLock);
	this->data.clear();
}

void DbIndex::RangeIndex_t::finish()
{
	this->createdAt = std::chrono::duration_cast<std::chrono::seconds>(
		std::chrono::system_clock::now().time_since_epoch() // Since 1970
		).count();
}

void DbIndex::RangeIndex_t::addItem(const nlohmann::json& item)
{
	if(!item.contains(this->perfomedOnKey))
		return;

	// Parse Value and insert it		
	try {
		auto value = item[this->perfomedOnKey];

		std::unique_lock<std::mutex> lockGuard(this->useLock);
		if (value.is_number())
			this->data.insert(std::make_pair(value.get<float>(), item["id"].get<size_t>()));
		else if (value.is_string())
			this->data.insert(std::make_pair(std::stof(value.get<std::string>()), item["id"].get<size_t>()));
	}
	catch (int code) {}
}

void DbIndex::RangeIndex_t::perform(float lowerBound, float higherBound, std::vector<size_t>& results)
{
	while (this->inBuilding)
		std::this_thread::sleep_for(std::chrono::nanoseconds(25));
	std::unique_lock<std::mutex> lockGuard(this->useLock);

	auto itLower = this->data.lower_bound(lowerBound);
	auto itHigher = this->data.upper_bound(higherBound);

	for (auto it = itLower; it != itHigher; ++it)
		results.push_back(it->second);
}

nlohmann::json DbIndex::RangeIndex_t::saveMetadata()
{
	nlohmann::json metadata;

	metadata["type"] = DbIndex::IndexType::RangeIndex;
	metadata["keyName"] = this->perfomedOnKey;

	return metadata;
}


//	*** Metadata Saver/Loader
DbIndex::Iindex_t* DbIndex::loadIndexFromJSON(const nlohmann::json& metadata)
{
	DbIndex::Iindex_t* index = nullptr;
	const std::string indexName = metadata["name"].get<std::string>();
	const DbIndex::IndexType indexType = metadata["type"].get<DbIndex::IndexType>();

	switch (indexType) {
	case DbIndex::IndexType::KeyValueIndex:
		index = new DbIndex::KeyValueIndex_t(metadata["keyName"].get<std::string>(), metadata["isHashedIndex"].get<bool>());
		break;
	case DbIndex::IndexType::MultipleKeyValueIndex:
		index = new DbIndex::MultipleKeyValueIndex_t(metadata["keyNames"], metadata["isFullHashedIndex"].get<bool>(), metadata["isHashedIndex"]);
		break;
	case DbIndex::IndexType::KnnIndex:
		index = new DbIndex::KnnIndex_t(metadata["keyName"].get<std::string>(), metadata["space"].get<size_t>());
		break;
	case DbIndex::IndexType::RangeIndex:
		index = new DbIndex::RangeIndex_t(metadata["keyName"].get<std::string>());
		break;
	}

	return index;
}

std::vector<nlohmann::json> DbIndex::saveIndexesToString(std::map<std::string, DbIndex::Iindex_t*>& indexes)
{
	std::vector<nlohmann::json> savedIndexes = {};
	
	for (const auto& pair : indexes) {
		nlohmann::json metadata = pair.second->saveMetadata();
		metadata["name"] = pair.first;
		
		savedIndexes.push_back(metadata);
	}

	return savedIndexes;
}


namespace CollectionFunctions {
	void performTTLCheck(const collection_t* col) {
		// Iterate through every storage and document to check wheter the &ttl field exists
		
		auto nowTimeSeconds = std::chrono::duration_cast<std::chrono::seconds>(
			std::chrono::system_clock::now().time_since_epoch() // Since 1970
		).count();

		for (const auto& storage : col->storage) {
			std::vector<size_t> ttlExpired = {}; // document ids

			// Check all documents
			storage->doFuncOnAllDocuments([&nowTimeSeconds, &ttlExpired](const nlohmann::json& document) {
				if (!document.contains("&ttl"))
					return; // Field does not exist


				// Calc diff
				auto docTTLSeconds = document["&ttl"].get<long long>();
				long long diff = nowTimeSeconds - docTTLSeconds;
				
				// If nowTime is greater than docTime ==> diff > 0 ==> Document's TTL is     expired
				// If nowTime is   less  than docTime ==> diff < 0 ==> Document's TTL is NOT expired

				if (diff > 0) // Expired
					ttlExpired.push_back(document["id"].get<size_t>());
			});

			// Remove expired ones
			for (const size_t& id : ttlExpired)
				storage->removeDocument(id);

			if (ttlExpired.size()) // Save if something happend
				storage->save();
		}
	}


	void runCircle() {
		while (!INTERRUPT) {
			// Do TTL Check
			for (const auto& item : collections)
				performTTLCheck(item.second);

			std::this_thread::sleep_for(std::chrono::minutes(5));
		}
	}

	void StartManagerThread() {
		managerThread = std::thread(runCircle);
	}
}

