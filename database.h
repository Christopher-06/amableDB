#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <vector>
#include <map>
#include <set>
#include <mutex>

#include <nlohmann/json.hpp>

#include "hnswlib.h"
#include "storage.h"


namespace DbIndex {
	enum class IndexType {
		KeyValueIndex, 
		MultipleKeyValueIndex,
		KnnIndex
	};

	class Iindex_t { // Base Class
	public:
		unsigned int createdAt;
		bool isInWork;
		std::mutex buildLock;

		virtual void buildIt(std::vector<group_storage_t*> storage) = 0;
		virtual std::set<std::string> getIncludedKeys() = 0;
		virtual nlohmann::json saveMetadata() = 0;
		virtual DbIndex::IndexType getType() = 0;
	};

	class KeyValueIndex_t : public Iindex_t {
	public:
		std::string perfomedOnKey;
		bool isHashedIndex;
		std::map<nlohmann::json, std::vector<size_t>> data; // 1. hash of value 2. id of documents

		KeyValueIndex_t(std::string keyName, bool isHashedIndex = false);
		std::vector<std::vector<size_t>> perform(std::vector<std::string>& values);

		nlohmann::json saveMetadata();
		std::set<std::string> getIncludedKeys();
		void buildIt(std::vector<group_storage_t*> storage);
		DbIndex::IndexType getType() { return DbIndex::IndexType::KeyValueIndex; };
	};

	class MultipleKeyValueIndex_t : public Iindex_t {
	private:
		std::vector<std::string> keyNames;
		bool isFullHashedIndex = false;
		std::vector<bool> isHashedIndex;
	public:
		std::vector<KeyValueIndex_t*> indexes;

		MultipleKeyValueIndex_t(std::vector<std::string> keyNames, bool isFullHashedIndex = false, std::vector<bool> isHashedIndex = {});
		std::vector<size_t> perform(std::map<std::string, std::vector<std::string>>& query);

		nlohmann::json saveMetadata();
		std::set<std::string> getIncludedKeys();
		void buildIt(std::vector<group_storage_t*> storage);
		DbIndex::IndexType getType() { return DbIndex::IndexType::MultipleKeyValueIndex; };
	};

	class KnnIndex_t : public Iindex_t {
	private:
		hnswlib::AlgorithmInterface<float>* index;
		hnswlib::L2Space space = hnswlib::L2Space(0);
		size_t spaceValue;
		std::string perfomedOnKey;

	public:
		KnnIndex_t(std::string keyName, size_t space);

		std::vector<std::vector<size_t>> perform(std::vector<std::vector<float>>& query, size_t limit);
		void perform(const std::vector<float>&, std::priority_queue<std::pair<float, hnswlib::labeltype>>*, size_t limit);

		nlohmann::json saveMetadata();
		std::set<std::string> getIncludedKeys();
		void buildIt(std::vector<group_storage_t*> storage);
		DbIndex::IndexType getType() { return DbIndex::IndexType::KnnIndex; };
	};

	Iindex_t* loadIndexFromJSON(const nlohmann::json& metadata);
	std::vector<nlohmann::json> saveIndexesToString(std::map<std::string, DbIndex::Iindex_t*>& indexes);
}

class collection_t {
public:
	std::string name;
	std::vector<group_storage_t*> storage;
	std::map<std::string, DbIndex::Iindex_t*> indexes;
	std::mutex saveLock;

	collection_t(std::string);
	std::vector<size_t> insertDocuments(const std::vector<nlohmann::json> documents);
	std::set<std::tuple<std::string, DbIndex::IndexType, DbIndex::Iindex_t*>> getIndexedKeys();

	size_t countDocuments();
};

void loadDatabase(std::string);
void loadCollection(std::string);

void saveDatabase(std::string dataPath);

inline std::map<std::string, collection_t*> collections = {};


#endif
