cmake_minimum_required(VERSION 3.22)

project( pkvs LANGUAGES CXX )

include( CTest )

find_package(
  Seastar
  REQUIRED
  PATHS /home/domen/dev/libs/seastar/lib/cmake/Seastar
  NO_DEFAULT_PATH)

find_package(
  nlohmann_json
  3.2.0
  REQUIRED
  PATHS /home/domen/dev/build/nlohmann
  NO_DEFAULT_PATH)

add_executable(
  ${PROJECT_NAME}
  pkvs/pkvs.cpp
  main.cpp
)
target_compile_features(
  ${PROJECT_NAME}
  PRIVATE
  cxx_std_23
)
target_link_libraries(
  ${PROJECT_NAME}
  Seastar::seastar
  nlohmann_json::nlohmann_json
)
