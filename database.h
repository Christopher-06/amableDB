#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <vector>
#include <map>
#include <set>


#include <nlohmann/json.hpp>

#include "storage.h"


namespace DbIndex {
	enum IndexType {
		KeyValueIndex, 
		MultipleKeyValueIndex
	};

	class Iindex_t { // Base Class
	public:
		unsigned int createdAt;
		bool isInWork;

		virtual void buildIt(std::vector<group_storage_t*> storage) = 0;
		virtual std::set<std::string> getIncludedKeys() = 0;
		virtual nlohmann::json saveMetadata() = 0;
	};

	class KeyValueIndex_t : public Iindex_t {
	public:
		std::string perfomedOnKey;
		bool isHashedIndex;
		std::map<std::string, std::vector<size_t>> data; // 1. hash of value 2. id of documents

		KeyValueIndex_t(std::string keyName, bool isHashedIndex = false);
		std::vector<std::vector<size_t>> perform(std::vector<std::string>& values);

		nlohmann::json saveMetadata();
		std::set<std::string> getIncludedKeys();
		void buildIt(std::vector<group_storage_t*> storage);
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
	};

	Iindex_t* loadIndexFromJSON(nlohmann::json& metadata);
	std::vector<nlohmann::json> saveIndexesToString(std::map<std::string, DbIndex::Iindex_t*>& indexes);
}

class collection_t {
public:
	std::string name;
	std::vector<group_storage_t*> storage;
	std::map<std::string, DbIndex::Iindex_t*> indexes;

	collection_t(std::string);
	size_t insertDocument(nlohmann::json& document);
};

void loadDatabase(std::string);
void loadCollection(std::string);

void saveDatabase(std::string dataPath);

inline std::map<std::string, collection_t*> collections = {};


#endif
