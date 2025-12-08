#include "ulid.hpp"
#include <fmt/format.h>

ULID ULID::empty {std::array<uint8_t, ULID::LENGTH>{0}};

// Static generator for new ULIDs
std::array<uint8_t, 16> ULID::ULIDGenerator::generate() const {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch())
                         .count();

    std::array<uint8_t, 16> ulid_bytes = {};

    // Timestamp (48 bits = 6 bytes) - big endian
    for (int i = 5; i >= 0; --i) {
        ulid_bytes[i] = static_cast<uint8_t>(timestamp & 0xFF);
        timestamp >>= 8;
    }

    // Randomness with monotonic guarantee
    generateRandomness(ulid_bytes, now);
    return ulid_bytes;
}

void ULID::ULIDGenerator::generateRandomness(std::array<uint8_t, 16>& ulid_bytes, const std::chrono::system_clock::time_point& now) const {
    uint64_t current_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    if (current_timestamp == last_timestamp) {
        // Same millisecond: increment last randomness for monotonic ordering
        incrementRandomness();
        std::copy(last_randomness.begin(), last_randomness.end(),
            ulid_bytes.begin() + 6);
    } else {
        // New millisecond: generate fresh randomness
        for (int i = 0; i < 10; ++i) {
            last_randomness[i] = static_cast<uint8_t>(dis(gen) & 0xFF);
            ulid_bytes[6 + i] = last_randomness[i];
        }
        last_timestamp = current_timestamp;
    }
}

void ULID::ULIDGenerator::incrementRandomness() const {
    for (int i = 9; i >= 0; --i) {
        if (++last_randomness[i] != 0) {
            break; // No carry needed
        }
    }
}
ULID::ULID(const std::string& ulid_string, bool isBinary) {
    if (ulid_string.empty()) {
        *this = empty;
        return;
    }
    if (isBinary) {
        if (ulid_string.length() != 16) throw std::invalid_argument(fmt::format("Binary string length not equal to 16, submitted {}", ulid_string.length()));
        std::memcpy(binary_data.data(), to_binary(ulid_string), 16);
        return;
    }
    if (ulid_string.length() != 26) {
        throw std::invalid_argument("Invalid ULID string length");
    }

    binary_data = {};
    uint64_t accumulator = 0;
    int bits_accumulated = 0;
    int byte_index = 0;

    for (char c : ulid_string) {
        if (c < 0 || c >= 128 || DECODING[c] == -1) {
            throw std::invalid_argument("Invalid character in ULID string");
        }

        accumulator = (accumulator << 5) | DECODING[c];
        bits_accumulated += 5;

        while (bits_accumulated >= 8 && byte_index < 16) {
            bits_accumulated -= 8;
            binary_data[byte_index++] = static_cast<uint8_t>((accumulator >> bits_accumulated) & 0xFF);
        }
    }
}

/**
 * Convert to string representation (for display/logging)
 * @return 26-character ULID string
 */
std::string ULID::toString() const {
    std::string result;
    result.reserve(26);

    uint64_t accumulator = 0;
    int bits_accumulated = 0;

    for (uint8_t byte : binary_data) {
        accumulator = (accumulator << 8) | byte;
        bits_accumulated += 8;

        while (bits_accumulated >= 5) {
            bits_accumulated -= 5;
            result.push_back(ENCODING[(accumulator >> bits_accumulated) & 0x1F]);
        }
    }

    if (bits_accumulated > 0) {
        result.push_back(ENCODING[(accumulator << (5 - bits_accumulated)) & 0x1F]);
    }

    result.resize(26, '0');
    return result;
}

std::string toString(std::unordered_set<ULID> m) {
    std::string res {"{"}, delim;
    for (auto x : m) {
        res.append(delim);
        res.append(x.toString());
        delim = ", ";
    }
    res.append("}");
    return res;
}

bool foundDuplicate(const std::unordered_set<ULID>& set, const ULID& key) {
    if (set.empty()) return false;
    if (set.find(key) == set.end()) return true;  // key not there but size>0 => other key is there!
    return set.size() > 1;                        // found key is in the set but, there's also another key
}

/**
 * Extract timestamp from ULID
 * @return Timestamp in milliseconds since epoch
 */
uint64_t ULID::timestamp() const {
    uint64_t ts = 0;
    for (int i = 0; i < 6; ++i) {
        ts = (ts << 8) | binary_data[i];
    }
    return ts;
}

/**
 * Convert to hexadecimal string for debugging
 * @return 32-character hex string
 */
std::string ULID::toHex() const {
    std::string result;
    result.reserve(32);
    const char hex_chars[] = "0123456789abcdef";

    for (uint8_t byte : binary_data) {
        result.push_back(hex_chars[byte >> 4]);
        result.push_back(hex_chars[byte & 0x0F]);
    }
    return result;
}

std::istream& operator>>(std::istream& is, ULID &ulid) {
    std::string str;
    is >> str;
    ulid = ULID(str);
    return is;
}

/**
 * Generate ULID with specific timestamp
 * @param timestamp_ms Timestamp in milliseconds since epoch
 * @return ULID with specified timestamp
 */
ULID ULID::generate(uint64_t timestamp_ms) {
    std::array<uint8_t, 16> ulid_bytes = {};

    // Set timestamp (48 bits = 6 bytes)
    uint64_t ts = timestamp_ms;
    for (int i = 5; i >= 0; --i) {
        ulid_bytes[i] = static_cast<uint8_t>(ts & 0xFF);
        ts >>= 8;
    }

    // Generate random bytes
    thread_local std::random_device rd;
    thread_local std::mt19937_64 gen(rd());
    thread_local std::uniform_int_distribution<uint64_t> dis(0, UINT64_MAX);

    for (int i = 6; i < 16; ++i) {
        ulid_bytes[i] = static_cast<uint8_t>(dis(gen) & 0xFF);
    }

    return ULID(ulid_bytes);
}

/**
 * Create ULID from hex string
 * @param hex_string 32-character hex string
 * @return ULID instance
 */
ULID ULID::fromHex(const std::string& hex_string) {
    if (hex_string.length() != 32) {
        throw std::invalid_argument("Invalid hex string length");
    }

    std::array<uint8_t, 16> binary_data;
    for (size_t i = 0; i < 16; ++i) {
        std::string byte_str = hex_string.substr(i * 2, 2);
        binary_data[i] = static_cast<uint8_t>(std::stoul(byte_str, nullptr, 16));
    }

    return ULID(binary_data);
}

/**
 * Validate ULID string format
 * @param ulid_string String to validate
 * @return true if valid format
 */
bool ULID::isValidString(const std::string& ulid_string){
    if (ulid_string.length() != 26) {
        return false;
    }

    for (char c : ulid_string) {
        if (c < 0 || c >= 128 || DECODING[c] == -1) {
            return false;
        }
    }

    return true;
}

// Static member definitions
constexpr char ULID::ENCODING[];

const int8_t ULID::DECODING[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, 16, 17, -1, 18, 19, -1, 20, 21, -1,
    22, 23, 24, 25, 26, -1, 27, 28, 29, 30, 31, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, 16, 17, -1, 18, 19, -1, 20, 21, -1,
    22, 23, 24, 25, 26, -1, 27, 28, 29, 30, 31, -1, -1, -1, -1, -1
};

thread_local ULID::ULIDGenerator ULID::generator;
