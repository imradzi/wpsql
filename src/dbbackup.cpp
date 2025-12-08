#include <string>
#include <filesystem>
#include "rDb.h"

#include "ZIP.h"
#include "logging.hpp"

namespace fs = std::filesystem;

static constexpr int numOfDailyBackup = 7;
static void DoTrimWeeklyFolder(const std::string &folderName) {
    // keep only latest 7 zip file
    LOG_INFO("Trimming weekly backup folder {}", folderName);
    std::vector<std::string> filenameList;
    for (auto& e : fs::directory_iterator {folderName}) {
        if (e.is_regular_file()) {
            if (boost::iequals(e.path().extension().string(), ".zip")) {
                std::string filename = e.path().string();
                filenameList.emplace_back(filename);
            }
        }
    }
    std::sort(filenameList.begin(), filenameList.end());
    int cnt = 0;
    for (std::vector<std::string>::reverse_iterator it = filenameList.rbegin(); it != filenameList.rend(); it++, cnt++) {  // cannot use reverse range
        if (cnt >= numOfDailyBackup) {
            LOG_INFO("Removing old file {}", *it);
            fs::remove(*it);
        }
    }
}

static auto GetDateFromString(const std::string &t) {
    std::string dateString;
    for (auto x : t) {
        if (std::isdigit(x)) dateString.push_back(x);
    }
    return EPOCHtoChrono(std::atoll(dateString.c_str()));
}

std::string GetEpoch() {
    auto time_now = std::chrono::system_clock().now();
    return std::to_string(std::chrono::duration_cast<std::chrono::seconds>(time_now.time_since_epoch()).count());
}

extern void LogStrFile(const std::string &filename, const std::string &content);

bool DB::SQLiteBase::BackupDB(int noOfBackupToKeep, std::string folderName, std::function<bool()> fnIsStopping) const {
    if (!folderName.empty() && folderName.back() != std::filesystem::path::preferred_separator)
        folderName.push_back(std::filesystem::path::preferred_separator);

    auto fileName = fs::path(GetDBName()).filename().string();
    auto tFileName = fs::path(GetTransactionDBName()).filename().string();

    try {
        auto startBackupStr = fmt::format("master={} trans={} Backup starts in {} m={} t={}", GetDBName(), GetTransactionDBName(), folderName, fileName, tFileName);
        LogStrFile(folderName + "start.txt", startBackupStr);
        CreateNonExistingFolders(folderName + fileName);
        CreateNonExistingFolders(folderName + std::string("Weekly") + std::string(1, std::filesystem::path::preferred_separator) + fileName);

        if (fnIsStopping && fnIsStopping()) return false;
        {
            LOG_INFO("Hourly Backup Master started. {}", GetDBName());
            wpSQLDatabase toBackup;
            toBackup.Open(GetDBName());
            if (toBackup.BackupTo(folderName + fileName, fnIsStopping) != SQLITE_OK) return false;
            LOG_INFO("Hourly Backup Master completed. {}", GetDBName());
        }
        if (fnIsStopping && fnIsStopping()) return false;
        if (!tFileName.empty()) {
            LOG_INFO("Hourly Backup Transaction started. {}", GetTransactionDBName());
            wpSQLDatabase toBackup;
            toBackup.Open(GetTransactionDBName());
            if (toBackup.BackupTo(folderName + tFileName, fnIsStopping) != SQLITE_OK) return false;
            LOG_INFO("Hourly Backup Transaction completed. {}", GetTransactionDBName());
        }

        if (fnIsStopping && fnIsStopping()) return false;
        {
            LOG_INFO("Zipping backup started.");
            auto f = folderName + "backup" + GetEpoch() + ".zip";
            Partio::ZipFileWriter zip(f);
            {
                std::ostream* o = zip.Add_File(fileName);
                std::ifstream in(folderName + fileName, std::ios::in | std::ios::binary);
                LOG_INFO("Zipping {} {}", folderName, fileName);
                size_t size {50000};
                while (!in.eof()) {
                    std::string buf(size, '\0');
                    in.read(&buf[0], size);
                    o->write(buf.data(), in.gcount());
                }
                o->flush();
                delete o;
            }
            {
                std::ostream* o = zip.Add_File(tFileName);
                std::ifstream in(folderName + tFileName, std::ios::in | std::ios::binary);
                LOG_INFO("Zipping {} {}", folderName, tFileName);
                size_t size {50000};
                while (!in.eof()) {
                    std::string buf(size, '\0');
                    in.read(&buf[0], size);
                    o->write(buf.data(), in.gcount());
                }
                o->flush();
                delete o;
            }
            LOG_INFO("Zipping backup completed.");
        }
        // keep only latest 2 zip file
        std::vector<std::string> filenameList;
        for (auto& e : fs::directory_iterator {folderName}) {
            if (e.is_regular_file()) {
                if (boost::iequals(e.path().extension().string(), ".zip")) {
                    std::string filename = e.path().string();
                    filenameList.emplace_back(filename);
                }
            }
        }
        std::sort(filenameList.begin(), filenameList.end());
        int cnt = 0;
        auto time_now = std::chrono::system_clock().now();
        bool needTrimWeekly = false;
        for (std::vector<std::string>::reverse_iterator it = filenameList.rbegin(); it != filenameList.rend(); it++, cnt++) {  // cannot use reverse range
            if (cnt >= noOfBackupToKeep) {
                auto fileName = fs::path(*it).filename().string();
                auto t = GetDateFromString(fileName);
                if (GetBeginOfDay(t) != GetBeginOfDay(time_now)) {
                    needTrimWeekly = true;
                    std::string newName = folderName + std::string("Weekly") + std::string(1, fs::path::preferred_separator) + fileName;
                    LOG_INFO("renaming {} to {}", *it, newName);
                    fs::rename(*it, newName);
                } else {
                    fs::remove(*it);
                }
            }
        }
        if (needTrimWeekly)
            DoTrimWeeklyFolder(folderName + std::string("Weekly") + std::string(1, fs::path::preferred_separator));
        LogStrFile(folderName + "finish.txt", startBackupStr);
        return true;
    } catch (std::exception& e) {
        LOG_ERROR("Backup error: {}", e.what());
    } catch (wpSQLException& e) {
        LOG_ERROR("Backup sql error: {}", e.message);
    } catch (...) {
        LOG_ERROR("Backup error: unknown!");
    }
    return false;
}
