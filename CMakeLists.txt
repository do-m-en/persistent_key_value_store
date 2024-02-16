cmake_minimum_required(VERSION 3.22)

project( pkvs LANGUAGES CXX )

include( CTest )

set(ENABLE_SANITIZERS FALSE CACHE BOOL "Should we build with sanitizers.")

if( ENABLE_SANITIZERS )
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fsanitize=address -fsanitize=undefined")
  set(CMAKE_LINKER_FLAGS "${CMAKE_STATIC_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=address -fsanitize=undefined")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

set(Seastar_DIR "" CACHE PATH "Location where SeastarConfig.cmake is located.")
set(nlohmann_json_DIR "" CACHE PATH "Location where nlohmann_jsonConfig.cmake is located.")

find_package(Seastar REQUIRED)
find_package(nlohmann_json REQUIRED)

add_executable(
  ${PROJECT_NAME}
  pkvs/pkvs.cpp
  pkvs/detail/sstables.cpp
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

foreach(
  test

  IN ITEMS

  add
  add_value_missing
  delete
  delete_non_existing
  persistency_test_shard_count_change
  run_on_all_cores
  sorted_keys
  sorted_keys_after_delete
  sorted_keys_empty
  update )
  add_test(
    NAME ${test}
    COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/pkvs/tests/${test}.sh"
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  )
endforeach()