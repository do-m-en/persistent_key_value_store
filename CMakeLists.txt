cmake_minimum_required(VERSION 3.22)

project( pkvs LANGUAGES CXX )

include( CTest )

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fsanitize=address -fsanitize=undefined")
#set(CMAKE_LINKER_FLAGS "${CMAKE_STATIC_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=address -fsanitize=undefined")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

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

foreach(
  test

  IN ITEMS

  add
  add_value_missing
  delete
  delete_non_existing
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