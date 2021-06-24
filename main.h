#ifndef MAIN_H
#define MAIN_H

#include <string>
#include <boost/program_options.hpp>

#pragma region GLOBAL-Settings

inline bool INTERRUPT = false;

inline std::string DATA_PATH = "V:\\CPP\\amableDB\\data";
inline int API_PORT = 3399;
inline std::string API_ADDRESS = "127.0.0.1";
inline bool AUTHENTICATION_ACTIVE = false;

inline size_t MAX_ELEMENTS_IN_STORAGE = 50000;

#pragma endregion



void parseStartArguments(int argc, char* argv[]);

inline std::set<std::string> TrueWritings = { "true", "True", "1", "yes", "Yes", "on", "On" };

#endif