cmake_minimum_required (VERSION 3.8)
project ("amableDB")

# Find: Boost
include_directories(${BOOST_ROOT})
find_package(Boost REQUIRED COMPONENTS program_options system) 
message(STATUS "Boost is found at: ${BOOST_ROOT} | ${Boost_INCLUDE_DIRS} | ${Boost_LIBRARY_DIRS}")
include_directories(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS}) 

# Find: third-parties
find_package(nlohmann_json REQUIRED)


# Find: Subdirectories
add_subdirectory("hnswlib")
add_subdirectory("hashing") 
add_subdirectory("Simple-Web-Server") 

add_executable(${PROJECT_NAME}  "main.h" "main.cpp" "database.h" "database.cpp" "storage.cpp" "api.h" "api.cpp"  
								"hashing/sha256.h" "hashing/sha256.cpp"  
								"CRUD/crud.h" "CRUD/create.cpp" "CRUD/select.cpp" "CRUD/update.cpp" "CRUD/remove.cpp")

# Enable C++17 standard compliance 
set_property (TARGET ${PROJECT_NAME}
  PROPERTY CXX_STANDARD 17
)

target_link_libraries(${PROJECT_NAME} hnswlib)
target_link_libraries(${PROJECT_NAME} hashing)
target_link_libraries(${PROJECT_NAME} simple-web-server)
target_link_libraries(${PROJECT_NAME} ${Boost_LIBRARIES} ${Boost_SYSTEM_LIBRARY} ${Boost_FILESYSTEM_LIBRARY})
target_link_libraries(${PROJECT_NAME} Boost::boost Boost::system Boost::program_options)
target_link_libraries(${PROJECT_NAME} nlohmann_json)