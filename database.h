#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <vector>
#include <map>
#include <set>
#include <mutex>
#include <thread>
#include <chrono>

#include <nlohmann/json.hpp>

#include "hnswlib.h"
#include "storage.h"


namespace DbIndex {
	enum class IndexType {
		KeyValueIndex, 
		MultipleKeyValueIndex,
		KnnIndex,
		RangeIndex
	};

	class Iindex_t { // Base Class
	public:
		unsigned int createdAt;
		std::atomic<bool> inBuilding = false;
		std::mutex useLock;

		virtual ~Iindex_t() {};

		virtual void reset() = 0;
		virtual void finish() = 0;
		virtual void addItem(const nlohmann::json& item) = 0;


		virtual std::set<std::string> getIncludedKeys() = 0;
		virtual nlohmann::json saveMetadata() = 0;
		virtual DbIndex::IndexType getType() = 0;
	};

	class KeyValueIndex_t : public Iindex_t {
	private:
		bool isHashedIndex;
		std::map<nlohmann::json, std::vector<size_t>> data; // 1. hash of value 2. id of documents
	
	public:
		std::string perfomedOnKey;

		KeyValueIndex_t(std::string keyName, bool isHashedIndex = false);
		~KeyValueIndex_t() {};

		inline void reset();
		inline void finish();
		inline void addItem(const nlohmann::json& item);
		std::vector<std::vector<size_t>> perform(std::vector<std::string>& values);

		nlohmann::json saveMetadata();
		std::set<std::string> getIncludedKeys();
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
		~MultipleKeyValueIndex_t() {};
		inline void reset();
		inline void finish();
		inline void addItem(const nlohmann::json& item);

		std::vector<size_t> perform(std::map<std::string, std::vector<std::string>>& query);

		nlohmann::json saveMetadata();
		std::set<std::string> getIncludedKeys();
		DbIndex::IndexType getType() { return DbIndex::IndexType::MultipleKeyValueIndex; };
	};

	class KnnIndex_t : public Iindex_t {
	private:
		hnswlib::AlgorithmInterface<float>* index;
		hnswlib::L2Space space = hnswlib::L2Space(0);
		size_t spaceValue;
		std::atomic<size_t> elementCount = 0;
		std::string perfomedOnKey;

	public:
		KnnIndex_t(std::string keyName, size_t space);
		~KnnIndex_t() { delete this->index; };

		std::vector<std::vector<size_t>> perform(std::vector<std::vector<float>>& query, size_t limit);
		void reset();
		inline void finish();
		inline void addItem(const nlohmann::json& item);

		void perform(const std::vector<float>&, std::priority_queue<std::pair<float, hnswlib::labeltype>>*, size_t limit);

		nlohmann::json saveMetadata();
		std::set<std::string> getIncludedKeys();
		DbIndex::IndexType getType() { return DbIndex::IndexType::KnnIndex; };
	};

	class RangeIndex_t : public Iindex_t {
	private:
		std::string perfomedOnKey;
		std::multimap<float, size_t> data;
	public:

		RangeIndex_t(std::string keyName);
		~RangeIndex_t() {};
		inline void reset();
		inline void finish();
		inline void addItem(const nlohmann::json& item);

		void perform(float lowerBound, float higherBound, std::vector<size_t>& results);

		nlohmann::json saveMetadata();
		std::set<std::string> getIncludedKeys() { return { this->perfomedOnKey }; };
		DbIndex::IndexType getType() { return DbIndex::IndexType::RangeIndex; };
	};

	Iindex_t* loadIndexFromJSON(const nlohmann::json& metadata);
	std::vector<nlohmann::json> saveIndexesToString(std::map<std::string, DbIndex::Iindex_t*>& indexes);
}


class collection_t {
private:
	std::mutex indexBuilderWaiting;
	std::mutex indexBuilderWorking;

public:
	std::string name;
	std::vector<group_storage_t*> storage;
	std::map<std::string, DbIndex::Iindex_t*> indexes;
	std::mutex saveLock;

	collection_t(std::string);
	std::vector<size_t> insertDocuments(const std::vector<nlohmann::json> documents);
	std::set<std::tuple<std::string, DbIndex::IndexType, DbIndex::Iindex_t*>> getIndexedKeys();
	void BuildIndexes();

	size_t countDocuments();
};

void loadDatabase(std::string);
void loadCollection(std::string);

void saveDatabase(std::string dataPath);

inline std::map<std::string, collection_t*> collections = {};

namespace CollectionFunctions {
	inline std::thread managerThread;

	void performTTLCheck(const collection_t* col);

	void runCircle();
	void StartManagerThread();
}

#endif
