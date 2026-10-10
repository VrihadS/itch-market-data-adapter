add_executable(test_byteorder test_byteorder.cpp)
target_link_libraries(test_byteorder PRIVATE itch)
add_test(NAME byteorder COMMAND test_byteorder)
