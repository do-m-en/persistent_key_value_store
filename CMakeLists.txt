cmake_minimum_required(VERSION 3.22)

project( pkvs LANGUAGES CXX )

include( CTest )

find_package(
  Seastar
  REQUIRED
  PATHS /home/domen/dev/libs/seastar/lib/cmake/Seastar
  NO_DEFAULT_PATH)

add_executable(
  ${PROJECT_NAME}
  main.cpp
)
target_compile_features(
  ${PROJECT_NAME}
  PRIVATE
  cxx_std_23
)
target_link_libraries( ${PROJECT_NAME} Seastar::seastar )


function(test_pkvs file_name)
  set(test_name "test__${file_name}")

  add_executable(
    ${test_name}
    pkvs/test/${file_name}.cpp )
  target_compile_features(
    ${test_name}
    PRIVATE
    cxx_std_23 )
  target_include_directories(
    ${test_name}
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/pkvs)

  add_test(
    NAME "${test_name}"
    COMMAND ${test_name} )
endfunction()

foreach(test IN ITEMS utf8_validated_view)
  test_pkvs( "${test}" )
endforeach()