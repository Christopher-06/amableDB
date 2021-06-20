#include <thread>
#include <nlohmann/json.hpp>

#include "crud.h"
#include "../main.h"
#include "../database.h"

void CRUD::update(nlohmann::json& request, nlohmann::json& response) {
	if (!request.contains("query") | !request.contains("update") | !request.contains("collection")) {
		response = { {"status", "failed"}, {"error", {"MissingKeys", "Query/Update/Collection is not send"}} };
		return;
	}

	// Parse Request
	nlohmann::json query = request["query"];
	const nlohmann::json update = request["update"];
	std::string collectionName = request["collection"].get<std::string>();

	size_t effectedDocuments = 0;
	std::vector<size_t> docIds;

	// Get Ids to work on
	try {
		SELECT::query_t queryPerformer(query, collectionName);
		queryPerformer.performQuery();

		// Get Results and extract Ids
		std::vector<std::tuple<size_t, float>> results;
		queryPerformer.exportResult(results);
		docIds.reserve(results.size());

		for (const auto [id, score] : results)
			docIds.push_back(id);
	}
	catch (nlohmann::json errorMsg) {
		std::cout << "Error raised while selecting:" << std::endl << errorMsg.dump() << std::endl;
		response = { {"status", "failed"}, {"QueryError", errorMsg}};
		return;
	}


	// Give storage the command to update them
	if (collections.count(collectionName)) {
		auto func = [&effectedDocuments, &docIds, &update](group_storage_t* storage) {
			for (const size_t& currentId : docIds) {
				if (!storage->savedHere(currentId))
					continue;

				++effectedDocuments;
				storage->editDocument(currentId, update);
			}
		};

		// Start Worker
		std::vector<std::thread> worker;
		for (const auto& storage : collections[collectionName]->storage)
			worker.push_back(std::thread(func, storage));

		// Wait for all to finish
		for (size_t i = 0; i < worker.size(); ++i)
		{
			if (worker[i].joinable())
				worker[i].join();
		}
	}

	response = { {"status", "ok"}, {"effectedDocuments", effectedDocuments} };

	// Save Database and perform Edits
	std::thread(saveDatabase, DATA_PATH).detach();
}


void UPDATE::performUpdate(const nlohmann::json& input, const nlohmann::json& update, nlohmann::json& output) {
	// Copy element
	output = nlohmann::json(input);

	// Perform Updates
	for (const auto& item : update.items()) {
		const std::string key = item.key();

		if (key[0] == '#') {
			// Operation Command
			auto func = UPDATE::Operator[key];
			nlohmann::json updateObj({ {key, item.value()} });
			func(output, updateObj);
		}
		else {
			// KeyValue Update
			output[key] = item.value();
		}
	}
}

void UPDATE::increaseOperator(nlohmann::json& document, nlohmann::json& update)
{
	const std::string keyName = update["#inc"]["key"].get<std::string>();
	const float incValue = update["#inc"]["value"].get<float>();

	// Get prevValue
	float prevValue = 0;
	if (document.contains(keyName) && document[keyName].is_number())
		prevValue = document[keyName].get<float>();

	document[keyName] = prevValue + incValue;
}
