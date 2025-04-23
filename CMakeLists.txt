cmake_minimum_required(VERSION 3.20)
project(json_to_orc)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING
      "Choose the type of build: Debug, Release, RelWithDebInfo, MinSizeRel"
      FORCE)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS
    ON CACHE BOOL "Generate compile_commands.json")

set(ORC_DIR "/home/annatar/repos/orc/build/lib/cmake/orc/")
set(ROOT_DIR "/home/annatar/studie/abi25/root/root-build"


find_package(ORC CONFIG REQUIRED
    COMPONENTS
        orc::orc
        ROOTNTuple
)

add_executable(json_to_orc json_to_orc.cpp)

target_link_libraries(json_to_orc
    PRIVATE
      orc::orc
      ROOT::ROOTNTuple
)
