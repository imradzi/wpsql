#pragma once

#include <string>
#include <chrono>
#include <random>
#include <array>
#include <algorithm>
#include <stdexcept>
#include <cctype>
#include <iostream>
#include <iomanip>
#include <cstring>
#include <unordered_set>

/**
 * ULID class with internal binary storage for optimal database performance
 *
 * Features:
 * - Stores ULID as 16-byte binary internally
 * - Converts to string only when needed (display, logging)
 * - Direct binary data access for database operations
 * - Efficient comparisons and sorting
 * - Type-safe operations
 *
 * Usage:
 *   ULID id;                           // Generate new ULID
 *   ULID id("01FJYWZ3RJM927XKDJGDR.."); // From string
 *   std::string str = id.toString();   // Convert to string
 *   auto binary = id.data();           // Get binary data for database
 */
class ULID {
public:
    static constexpr size_t LENGTH = 16;

private:
    static const uint8_t* to_binary(const std::string& s) { return reinterpret_cast<const uint8_t*>(s.data()); }

    std::array<uint8_t, LENGTH> binary_data;

    // Static encoding/decoding tables for Base32
    static constexpr char ENCODING[] = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    static const int8_t DECODING[128];

    // Static generator for new ULIDs
    static thread_local class ULIDGenerator {
    private:
        mutable std::random_device rd;
        mutable std::mt19937_64 gen;
        mutable std::uniform_int_distribution<uint64_t> dis;
        mutable uint64_t last_timestamp = 0;
        mutable std::array<uint8_t, 10> last_randomness = {};

    public:
        ULIDGenerator() : gen(rd()), dis(0, UINT64_MAX) {}
        std::array<uint8_t, LENGTH> generate() const;

    private:
        void generateRandomness(std::array<uint8_t, LENGTH>& ulid_bytes, const std::chrono::system_clock::time_point& now) const;
        void incrementRandomness() const;
    } generator;

public:
    ULID() : binary_data(generator.generate()) {}
    explicit ULID(const std::string& ulid_string, bool isBinary = true);
    explicit ULID(const std::array<uint8_t, LENGTH>& binary_ulid) : binary_data(binary_ulid) {}
    explicit ULID(const uint8_t* data) { std::memcpy(binary_data.data(), data, LENGTH); }
    ULID(const ULID& other) = default;
    ULID& operator=(const ULID& other) = default;
    std::string toString() const;
    std::string toBinary() const { return std::string(reinterpret_cast<const char*>(data()), size()); }
    const uint8_t* data() const { return binary_data.data(); }
    // const mysqlx::bytes getBytes() const { return { reinterpret_cast<const mysqlx::byte *>(binary_data.data()), LENGTH }; }
    std::pair<const uint8_t*, size_t> getBlob() const { return {binary_data.data(), LENGTH}; }
    const std::array<uint8_t, LENGTH>& binary() const { return binary_data; }
    constexpr size_t size() const { return LENGTH; }
    uint64_t timestamp() const;
    std::chrono::system_clock::time_point timePoint() const { return std::chrono::system_clock::time_point(std::chrono::milliseconds(timestamp())); }
    std::string toHex() const;
    bool isValid() const {
        return std::any_of(binary_data.begin(), binary_data.end(), [](uint8_t b) { return b != 0; });
    }
    static ULID empty;
    static bool isEmpty(const std::string& e) { return e.empty() || ULID(e) == empty; }
    bool isEmpty() const { return *this == empty; }

    // Comparison operators (efficient binary comparison)
    bool operator==(const ULID& other) const { return binary_data == other.binary_data; }
    bool operator!=(const ULID& other) const { return binary_data != other.binary_data; }
    bool operator<(const ULID& other) const { return binary_data < other.binary_data; }
    bool operator<=(const ULID& other) const { return binary_data <= other.binary_data; }
    bool operator>(const ULID& other) const { return binary_data > other.binary_data; }
    bool operator>=(const ULID& other) const { return binary_data >= other.binary_data; }

    static ULID generate() { return ULID(); }
    static ULID generate(uint64_t timestamp_ms);
    static ULID fromHex(const std::string& hex_string);
    static bool isValidString(const std::string& ulid_string);
};

std::string toString(std::unordered_set<ULID>);
bool foundDuplicate(const std::unordered_set<ULID>& set, const ULID& key);
inline std::ostream& operator<<(std::ostream& os, const ULID& ulid) { return os << ulid.toString(); }
std::istream& operator>>(std::istream& is, ULID& ulid);

// Hash support for std::unordered_map, std::unordered_set
namespace std {
    template<> struct hash<ULID> {
        size_t operator()(const ULID& ulid) const {
            // Use first 8 bytes as hash (sufficient for most use cases)
            const uint8_t* data = ulid.data();
            size_t result = 0;
            for (int i = 0; i < 8; ++i) { result = (result << 8) | data[i]; }
            return result;
        }
    };
}