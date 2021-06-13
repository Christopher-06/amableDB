#include <string>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>

#include "sha256.h"
#include "storage.h"

const std::string EMPTY_ROW_SEQUENCE = "<fgsngflwsitu948whg49ghwe98gh>";

group_storage_t::group_storage_t(std::string storagePath)
{
	this->locked = true;
	this->readingFile = false;
	this->storagePath = storagePath;
	this->idPositions = {};

	// Open storage file (will create it if needed)
	this->storageFile = std::fstream(this->storagePath);

	// Load documentIDs from storage-file and insert them into
	// idPositions for faster access
	std::string line;
	size_t index = 0;
	while (std::getline(this->storageFile, line)) {
		if (line != EMPTY_ROW_SEQUENCE) {
			nlohmann::json document = nlohmann::json::parse(line);
			auto id = document["id"];
			this->idPositions[document["id"].get<size_t>()] = index;
		}		
		++index;
	}
	storageFile.close();
	this->locked = false;
}

bool group_storage_t::savedHere(size_t documentID)
{
	// True if it is inside idPositions or inside newDocuments
	return (this->idPositions.count(documentID) || this->newDocuments.count(documentID));
}

void group_storage_t::getDocuments(std::vector<size_t>* ids, std::vector<nlohmann::json>& documents, bool allDocuments)
{
	documents.reserve(ids->size());
	while (this->locked)
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	this->locked = true;


	// Convert ids to block rows
	std::map<size_t, size_t> rows = {};
	for (auto it = ids->begin(); it != ids->end(); ++it) {
		if (this->idPositions.count(*it))
			rows[idPositions[*it]] = *it;
	}

	// Get documents from newDocuments
	for (auto it = ids->begin(); it != ids->end(); ++it) {
		if (this->newDocuments.count(*it) || allDocuments)
			documents.push_back(this->newDocuments[*it]);
	}

	// Get documents from storage file
	this->storageFile = std::fstream(this->storagePath);
	std::string line;
	size_t index = 0;
	while (std::getline(this->storageFile, line)) {
		++index;
		if (rows.count(index - 1) || allDocuments) {
			// Block row is selected ==> Parse JSON and set it
			documents.push_back(nlohmann::json::parse(line));

			if (documents.size() >= ids->size()) // Finished
				break;
		}
	}
	
	storageFile.close();
	this->locked = false;
}

size_t group_storage_t::countDocuments()
{
	return this->idPositions.size() + this->newDocuments.size();
}

void group_storage_t::insertDocument(const nlohmann::json& document)
{
	this->newDocuments[document["id"].get<size_t>()] = document;
}

void group_storage_t::doFuncOnAllDocuments(std::function<void(nlohmann::json&)> func)
{
	this->save(); // write all unsaved/edited things down

	while (this->locked) // Lock storage
		std::this_thread::sleep_for(std::chrono::milliseconds(2));
	this->locked = true;

	// Perform func on every object
	this->storageFile = std::fstream(this->storagePath);
	std::string line;
	while (std::getline(this->storageFile, line)) {
		if (line == EMPTY_ROW_SEQUENCE)
			continue;
		
		// Fire func on this document
		nlohmann::json lineDocument = nlohmann::json::parse(line);
		func(lineDocument);
	}
	

	// Finished --> unlock
	this->locked = false;
}

void group_storage_t::save()
{
	// Open a new file in append mode and write everything inside
	// Then set it as storagePath and delete old one
	// The new Filename is the hashed (old) filename

	if (!this->newDocuments.size() && !this->editedDocuments.size())
		return; // Nothing was changed

	while (this->locked) // Wait to lock storage file
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	this->locked = true;

	// Create random (/hashed) new filename
	std::string oldFilePath = this->storagePath.parent_path().u8string();
	std::string oldFileName = this->storagePath.filename().u8string();
	std::string newFilePath = oldFilePath + "\\" + sha256(oldFileName) + ".knndb";
	std::fstream newStorageFile = std::fstream(newFilePath, std::ios::app);

	// Open also old file
	this->storageFile = std::fstream(this->storagePath);
	std::string line;
	size_t lineIndex = 0;

	// Write it all down
	while (std::getline(this->storageFile, line)) {
		if (line == EMPTY_ROW_SEQUENCE) {
			// Is empty row ==> fill it with the first open one and set id to this position in idPositions 
			if(this->newDocuments.size()) {
				auto item = *(this->newDocuments.begin());
				newStorageFile << item.second.dump() << "\n";

				this->newDocuments.erase(item.first);
				this->idPositions[item.first] = lineIndex;
			}
			else {
				// Cannot find anything new ==> Use Empty Row Sequence
				newStorageFile << EMPTY_ROW_SEQUENCE << "\n";
			}
		}
		else {
			// Line contains an document
			if (this->editedDocuments.count(lineIndex)) {
				// This document got an update
				newStorageFile << this->editedDocuments[lineIndex] << "\n";
				this->editedDocuments.erase(lineIndex);
			}
			else {
				// This document is not touched
				newStorageFile << line << "\n";
			}
		
		}
		++lineIndex;
	}

	// Add new Documents (if some still there)
	for (const auto& item : this->newDocuments) {
		newStorageFile << item.second.dump() << "\n";
		this->idPositions[item.first] = lineIndex;
		++lineIndex;
	}
	this->newDocuments.clear();

	newStorageFile.close();
	this->storageFile.close();

	// Delete old file  and change storagePath
	std::filesystem::remove(this->storagePath);
	this->storagePath = newFilePath;

	// Finished ==> Unlock and continue work
	this->locked = false; 
}
