#include <thread>
#include <nlohmann/json.hpp>

#include "crud.h"
#include "../main.h"
#include "../database.h"

void CRUD::remove(nlohmann::json& request, nlohmann::json& response) {
	if (!request.contains("query") | !request.contains("collection")) {
		response = { {"status", "failed"}, {"error", {"MissingKeys", "Query/Collection is not send"}} };
		return;
	}

	// Parse request
	nlohmann::json query = request["query"];
	std::string collectionName = request["collection"].get<std::string>();
	size_t effectedDocs = 0;

	// Get Ids to remove
	std::vector<size_t> docIds;
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
		response = { {"status", "failed"}, {"QueryError", errorMsg} };
		return;
	}

	// Delete in every storage
	if (collections.count(collectionName)) {
		for(const auto& storage : collections[collectionName]->storage) {
			for (const auto& id : docIds) {
				if (storage->removeDocument(id))
					++effectedDocs; // Got removed
			}
				
		}
	}	

	response = { {"status", "ok"}, {"effectedDocuments", effectedDocs} };

	// Save Database and perform Removes
	std::thread(saveDatabase, DATA_PATH).detach();
}