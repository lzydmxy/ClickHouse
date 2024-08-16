#pragma once


namespace Consensus
{

UInt32 getCRC32(const char * data, size_t length);

bool verifyCRC32(const char * data, size_t len, uint32_t value);

UInt32 getChecksum(const char * data, uint32_t length);

bool verifyChecksum(const char * data, uint32_t len, uint32_t value);

std::string getCurrentTime();

}
