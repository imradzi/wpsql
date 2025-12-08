#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <fmt/format.h>
#include <memory>
#include <string>
#include <type_traits>

namespace  DB {

    /**
     * Centralized logging utility
     * Uses spdlog with simple formatting
     */
    class Logger {
    public:
        static void initialize(const std::string& serviceName, const std::string& logLevel = "info");
        static std::shared_ptr<spdlog::logger> get(const std::string& name = "default");

        // Request correlation ID for tracing across services
        static void setRequestId(const std::string& requestId);
        static std::string getRequestId();

        // Simple logging helpers
        static void info(const std::string& message);
        static void error(const std::string& message);
        static void warn(const std::string& message);
        static void debug(const std::string& message);

        // Audit logging for sensitive operations
        static void logAudit(const std::string& action, const std::string& entity, int entityId, int userId, const std::string& details = "");

    private:
        static std::shared_ptr<spdlog::logger> defaultLogger_;
        static thread_local std::string requestId_;
    };

}

// Template logging functions that use fmt::runtime for all format strings
template<typename... Args> 
inline void LOG_INFO(const std::string& format, Args&&... args) {
    if (auto logger = DB::Logger::get()) {
        logger->info(fmt::runtime(format), std::forward<Args>(args)...);
    }
}

template<typename... Args> 
inline void LOG_ERROR(const std::string& format, Args&&... args) {
    if (auto logger = DB::Logger::get()) {
        logger->error(fmt::runtime(format), std::forward<Args>(args)...);
    }
}

template<typename... Args> 
inline void LOG_WARN(const std::string& format, Args&&... args) {
    if (auto logger = DB::Logger::get()) {
        logger->warn(fmt::runtime(format), std::forward<Args>(args)...);
    }
}

template<typename... Args> 
inline void LOG_DEBUG(const std::string& format, Args&&... args) {
    if (auto logger = DB::Logger::get()) {
        logger->debug(fmt::runtime(format), std::forward<Args>(args)...);
    }
}

// LOG_AUDIT function with proper signature
inline void LOG_AUDIT(const std::string& action, const std::string& entity, int entityId, int userId, const std::string& details = "") {
    DB::Logger::logAudit(action, entity, entityId, userId, details);
}
