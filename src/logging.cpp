#include "logging.hpp"
#include <spdlog/pattern_formatter.h>
#include <chrono>
#include <random>
#include <iomanip>
#include <sstream>

namespace DB {

std::shared_ptr<spdlog::logger> Logger::defaultLogger_;
thread_local std::string Logger::requestId_;

void Logger::initialize(const std::string& serviceName, const std::string& logLevel) {
    auto logger = spdlog::get(serviceName);
    if (logger) {
        logger->set_level(spdlog::level::from_str(logLevel));
        return;
    }
    // Create console sink
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    
    // Create logger
    defaultLogger_ = std::make_shared<spdlog::logger>(serviceName, console_sink);
    defaultLogger_->set_level(spdlog::level::from_str(logLevel));
    
    // Set simple pattern
    defaultLogger_->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] [%l] %v");
    
    // Register as default
    spdlog::register_logger(defaultLogger_);
    spdlog::set_default_logger(defaultLogger_);
    
    info("Logger initialized for service: " + serviceName);
}

std::shared_ptr<spdlog::logger> Logger::get(const std::string& name) {
    if (name == "default") {
        return defaultLogger_;  // Can be nullptr if not initialized
    }
    
    auto logger = spdlog::get(name);
    if (!logger) {
        logger = defaultLogger_;  // Can be nullptr if not initialized
    }
    return logger;
}

void Logger::setRequestId(const std::string& requestId) {
    requestId_ = requestId;
}

std::string Logger::getRequestId() {
    if (requestId_.empty()) {
        // Generate a new request ID
        static thread_local std::random_device rd;
        static thread_local std::mt19937 gen(rd());
        static thread_local std::uniform_int_distribution<> dis(0, 15);
        
        std::ostringstream oss;
        for (int i = 0; i < 32; ++i) {
            if (i == 8 || i == 12 || i == 16 || i == 20) {
                oss << "-";
            }
            oss << std::hex << dis(gen);
        }
        requestId_ = oss.str();
    }
    return requestId_;
}

void Logger::info(const std::string& message) {
    get()->info(message);
}

void Logger::error(const std::string& message) {
    get()->error(message);
}

void Logger::warn(const std::string& message) {
    get()->warn(message);
}

void Logger::debug(const std::string& message) {
    get()->debug(message);
}

void Logger::logAudit(const std::string& action, const std::string& entity, 
                     int entityId, int userId, const std::string& details) {
    auto logger = get();
    
    std::ostringstream oss;
    oss << "AUDIT: action=" << action 
        << " entity=" << entity 
        << " entity_id=" << entityId 
        << " user_id=" << userId 
        << " details=" << details
        << " request_id=" << getRequestId();
    
    logger->info(oss.str());
}

}
