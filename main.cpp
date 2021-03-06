#include <iostream>
#include <boost/program_options.hpp>
#include <signal.h>

#include "main.h"
#include "database.h"
#include "api.h"
#include "CRUD/crud.h"

void parseStartArguments(int argc, char* argv[])
{
    namespace po = boost::program_options;

#pragma region Options-Description   
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Produce help message")
        ("apiPort", po::value<int>(), "Set API-Port to listen at")
        ("apiAddress", po::value<std::string>(), "Set API-Address to listen at")
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

    if (vm.count("apiAddress")) {
        API_ADDRESS = vm["apiAddress"].as<std::string>();
        std::cout << "[VAR] apiAddress was set to " << API_ADDRESS << std::endl;
    }
    
#pragma endregion

}

int main(int argc, char* argv[]) {  
    // Init
    parseStartArguments(argc, argv);
    signal(SIGINT, [](int signal) { INTERRUPT = true; }); // Control-C Event
    signal(SIGTERM, [](int signal) { INTERRUPT = true; }); // Docker-Stop Event

    // Start Up
    loadDatabase(DATA_PATH);
    API::startAPI(API_PORT, API_ADDRESS);
    CollectionFunctions::StartManagerThread();

    // Main Loop
    std::cout << "  --- Everything is running fine ---  " << std::endl;
    while (!INTERRUPT) {
        std::this_thread::sleep_for(std::chrono::minutes(3));
        
        saveDatabase(DATA_PATH); // save Database
        SELECT::cursor_t::removeLeftCursors();// remove left cursors
    } 

    // Exit
    std::cout << "  --- Shutting Database down ---  " << std::endl;
    saveDatabase(DATA_PATH);

    std::cout << "Good Bye" << std::endl;
    exit(0);
    return 0;
}

