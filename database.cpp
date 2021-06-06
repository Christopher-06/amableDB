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
			DbIndex::Iindex_t* index = DbIndex::loadIndexFromJSON(nlohmann::json(indexMeta));

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

size_t collection_t::insertDocument(nlohmann::json& document)
{
	if (!document.contains("id")) {
		// TODO: Create unused ID
		std::random_device dev;
		std::mt19937 rng(dev());
		std::uniform_int_distribution<std::mt19937::result_type> dist(1, 6);

		document["id"] = static_cast<size_t>(rng());
	}

	// Append in Storage Class
	for (const auto& storage : this->storage) {
		if (storage->countDocuments() >= MAX_ELEMENTS_IN_STORAGE)
			continue;

		// Got a Place
		storage->insertDocument(document);
		return document["id"].get<size_t>();
	}
	
	// Create new Storage Class with id as filename (because it is a unique start point)
	const std::string storagePath = DATA_PATH + "\\col_" + this->name + "\\storageNew" + std::to_string(document["id"].get<size_t>()) + ".knndb";
	group_storage_t* storage = new group_storage_t(storagePath);
	storage->insertDocument(document);
	this->storage.push_back(storage);

	return document["id"].get<size_t>();
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
			result.push_back(this->data[value]);
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
				std::string dataKey = document[this->perfomedOnKey].get<std::string>();
				if (this->isHashedIndex)
					dataKey = sha256(dataKey);

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



DbIndex::Iindex_t* DbIndex::loadIndexFromJSON(nlohmann::json& metadata)
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
