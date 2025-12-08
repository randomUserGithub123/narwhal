# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file LICENSE.rst or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION ${CMAKE_VERSION}) # this file comes with cmake

# If CMAKE_DISABLE_SOURCE_CHANGES is set to true and the source directory is an
# existing directory in our source tree, calling file(MAKE_DIRECTORY) on it
# would cause a fatal error, even though it would be a no-op.
if(NOT EXISTS "/home/nnescio/Documents/university/thesis/code/narwhal/Batch-OF/Themis_tx/secp256k1")
  file(MAKE_DIRECTORY "/home/nnescio/Documents/university/thesis/code/narwhal/Batch-OF/Themis_tx/secp256k1")
endif()
file(MAKE_DIRECTORY
  "/home/nnescio/Documents/university/thesis/code/narwhal/Batch-OF/Themis_tx/libsecp256k1-prefix/src/libsecp256k1-build"
  "/home/nnescio/Documents/university/thesis/code/narwhal/Batch-OF/Themis_tx/libsecp256k1-prefix"
  "/home/nnescio/Documents/university/thesis/code/narwhal/Batch-OF/Themis_tx/libsecp256k1-prefix/tmp"
  "/home/nnescio/Documents/university/thesis/code/narwhal/Batch-OF/Themis_tx/libsecp256k1-prefix/src/libsecp256k1-stamp"
  "/home/nnescio/Documents/university/thesis/code/narwhal/Batch-OF/Themis_tx/libsecp256k1-prefix/src"
  "/home/nnescio/Documents/university/thesis/code/narwhal/Batch-OF/Themis_tx/libsecp256k1-prefix/src/libsecp256k1-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/home/nnescio/Documents/university/thesis/code/narwhal/Batch-OF/Themis_tx/libsecp256k1-prefix/src/libsecp256k1-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/home/nnescio/Documents/university/thesis/code/narwhal/Batch-OF/Themis_tx/libsecp256k1-prefix/src/libsecp256k1-stamp${cfgdir}") # cfgdir has leading slash
endif()
