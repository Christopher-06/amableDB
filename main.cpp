#include <iostream>
#include <boost/program_options.hpp>

#include "main.h"
#include "database.h"
#include "api.h"

void parseStartArguments(int argc, char* argv[])
{
    namespace po = boost::program_options;

#pragma region Options-Description   
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Produce help message")
        ("apiPort", po::value<int>(), "Set API-Port to listen at")
        ("dataPath", po::value<std::string>(), "Path to the data Folder");
#pragma endregion

    // Parsing
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

#pragma region Valuating
    if (vm.count("help")) {
        std::cout << desc << "\n";
        exit(0);
    }

    if (vm.count("dataPath")) {
        DATA_PATH = vm["dataPath"].as<std::string>();
        std::cout << "[VAR] dataPath was set to " << DATA_PATH << std::endl;
    }

    if (vm.count("apiPort")) {
        API_PORT = vm["apiPort"].as<int>();
        std::cout << "[VAR] apiPort was set to " << API_PORT << std::endl;
    }
#pragma endregion

}

int main(int argc, char* argv[]) {
    parseStartArguments(argc, argv);

    loadDatabase(DATA_PATH);

    startAPI(API_PORT, API_ADDRESS);

    saveDatabase(DATA_PATH);
    return 0;
}