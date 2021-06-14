#include <iostream>
#include <fstream>
#include <filesystem>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <chrono>
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
	if (!std::filesystem::exists(collectionPath + "\\collection.metadata")) {
		std::cout << "Attention: Something went wrong! Collection may be corrupted" << std::endl;
		std::cout << "Collection cannot be loaded: " << collectionPath << " Aborted." << std::endl;
		return;
	}

	// Load and Parse metadata
	std::ifstream fs(collectionPath + "\\collection.metadata", std::fstream::in);
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
	for (const auto& file : std::filesystem::directory_iterator(collectionPath)) {
		if (file.path().filename().u8string().find(".knndb") == std::string::npos)
			continue; // No storage file knndb file
		group_storage_t* storage = new group_storage_t(file.path().u8string());
		col->storage.push_back(storage);
	}

	// Make indexes
	for (const auto& index : col->indexes)
		index.second->buildIt(col->storage);

	return;
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

	std::cout << "[INFO] Loaded all Collections and built all Indexes successfully" << std::endl;
}

void saveDatabase(std::string dataPath) {
	// Save Collections
	for (const auto& col : collections) {
		for (const auto storage : col.second->storage)
			storage->save();

		// Save Metadata to JSON-Object
		nlohmann::json dbMetadata;
		dbMetadata["name"] = col.first;
		dbMetadata["indexes"] = DbIndex::saveIndexesToString(col.second->indexes);

		// Write JSON down
		std::ofstream metadataFile (dataPath + "\\col_" + col.first + "\\collection.metadata", std::fstream::trunc);
		metadataFile << dbMetadata;
		metadataFile.close();
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
		const std::string storagePath = DATA_PATH + "\\col_" + this->name + "\\storageNew" + std::to_string(rng()) + ".knndb";
		storage = new group_storage_t(storagePath);
		this->storage.push_back(storage);
	}

	// Enter all Documents
	std::vector<size_t> entereredIds(documents.size());
	auto enteredIdIT = entereredIds.begin();
	for (auto doc : documents) {
		if (!doc.contains("id")) {
			// TODO: Create unused ID
			doc["id"] = static_cast<size_t>(rng());
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

size_t collection_t::countDocuments()
{
	size_t total = 0;

	for (const auto& storage : this->storage)
		total += storage->countDocuments();

	return total;
}


//	*** General Index ***

//	*** KeyValue Index ***
DbIndex::KeyValueIndex_t::KeyValueIndex_t(std::string keyName, bool isHashedIndex)
{
	this->perfomedOnKey = keyName;
	this->isHashedIndex = isHashedIndex;
	this->data = {};
	this->createdAt = 0;
	this->isInWork = false;
}

std::vector<std::vector<size_t>> DbIndex::KeyValueIndex_t::perform(std::vector<std::string>& values)
{
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

void DbIndex::KeyValueIndex_t::buildIt(std::vector<group_storage_t*> storage)
{
	this->isInWork = true;
	this->data = {};
	
	for (const auto& storagePtr : storage) {
		storagePtr->doFuncOnAllDocuments([this](nlohmann::json& document) {
			if (document.contains(this->perfomedOnKey)) {
				// document contains the key
				nlohmann::json dataKey = document[this->perfomedOnKey];
				if (this->isHashedIndex)
					dataKey = sha256(dataKey.dump());

				// Add dataKey to index-data
				if (this->data.count(dataKey))
					this->data[dataKey].push_back(document["id"].get<size_t>());
				else
					this->data[dataKey] = { document["id"].get<size_t>() };
			}
		});
	}

	this->isInWork = false;
	this->createdAt = std::chrono::duration_cast<std::chrono::seconds>(
		std::chrono::system_clock::now().time_since_epoch() // Since 1970
	).count();
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
	this->isInWork = false;
}

std::vector<size_t> DbIndex::MultipleKeyValueIndex_t::perform(std::map<std::string, std::vector<std::string>>& query) {
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

void DbIndex::MultipleKeyValueIndex_t::buildIt(std::vector<group_storage_t*> storage)
{
	this->isInWork = true;

	for (const auto& subIndex : this->indexes)
		subIndex->buildIt(storage);

	this->isInWork = false;
	this->createdAt = std::chrono::duration_cast<std::chrono::seconds>(
		std::chrono::system_clock::now().time_since_epoch() // Since 1970
		).count();
}

//	*** Knn Index ***
DbIndex::KnnIndex_t::KnnIndex_t(std::string keyName, size_t space)
{
	this->perfomedOnKey = keyName;
	this->index = nullptr;
	this->spaceValue = space;
	this->space = hnswlib::L2Space(space);
	this->createdAt = 0;
	this->isInWork = false;
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

std::priority_queue<std::pair<float, hnswlib::labeltype>> DbIndex::KnnIndex_t::perform(const std::vector<float>& query, size_t limit) {
	return this->index->searchKnn(query.data(), limit);
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

void DbIndex::KnnIndex_t::buildIt(std::vector<group_storage_t*> storage)
{
	this->isInWork = true;

	size_t total = 0; // Get total docs
	for (const auto& storagePtr : storage)
		total += storagePtr->countDocuments();
	
	delete this->index;
	this->index = new hnswlib::HierarchicalNSW<float>(&(this->space), total);

	// Get all documents and add them into the Index
	std::vector<float> data(this->spaceValue);
	for (const auto& storagePtr : storage) {
		storagePtr->doFuncOnAllDocuments([this, &data](nlohmann::json& document) {
			if (document.contains(this->perfomedOnKey)) {
				// document contains the key
				if (!document[this->perfomedOnKey].is_array())
					return;

				data = std::vector<float>(this->spaceValue);
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

				hnswlib::labeltype _id = document["id"].get<size_t>();
				this->index->addPoint(data.data(), _id);
			}
		});
	}

	this->isInWork = false;
	this->createdAt = std::chrono::duration_cast<std::chrono::seconds>(
		std::chrono::system_clock::now().time_since_epoch() // Since 1970
		).count();
}



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
