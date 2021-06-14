#ifndef STORAGE_H
#define STORAGE_H

#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_map>
#include <random>
#include <chrono>

#include <nlohmann/json.hpp>

namespace lock {
	class lock_item_t {
	public:
		std::function<void(size_t)> unlockFunc;
		size_t id;

		lock_item_t(std::function<void(size_t)> unlockFunc, size_t id) { this->unlockFunc = unlockFunc; this->id = id; };
		~lock_item_t() { this->unlockFunc(this->id); };
	};

	class lock_watcher_t {
	public:
		std::map<size_t, bool> queue; // lock id, canStart/isStarted/isRunning

		lock_item_t lock();
		void unlock(size_t id);
	};
}


class group_storage_t {
private:
	std::filesystem::path storagePath;
	std::fstream storageFile;
	lock::lock_watcher_t lockWatcher;
	std::unordered_map<size_t, size_t> idPositions; // 1. id of doc 2. row index
	std::unordered_map<size_t, nlohmann::json> newDocuments; // 1. id of doc 2. full document
	std::unordered_map<size_t, nlohmann::json> editedDocuments; // 1. row index 2. full (edited) document

public:
	group_storage_t(std::string storagePath);
	bool savedHere(size_t);
	void getDocuments(std::vector<size_t>* ids, std::vector< nlohmann::json>& documents, bool allDocuments = false);
	size_t countDocuments();
	void insertDocument(const nlohmann::json& document);

	void doFuncOnAllDocuments(std::function<void(nlohmann::json&)> func);
	void save();
};

#endif // !STORAGE_H
