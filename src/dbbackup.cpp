#include "precompiled/libcommon.h"
#ifdef _WIN32
#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>
#endif
#ifdef _WIN32
#include "winsock2.h"
#endif

#ifdef __clang__
#if __has_warning("-Wdeprecated-enum-enum-conversion")
#pragma clang diagnostic ignored "-Wdeprecated-enum-enum-conversion"  // warning: bitwise operation between different enumeration types ('XXXFlags_' and 'XXXFlagsPrivate_') is deprecated
#endif
#endif
#include <string>
#include <filesystem>
#ifdef __WX__
#include "wx/wxprec.h"
#ifndef WX_PRECOMP
#include "wx/wx.h"
#endif
#include "wx/xml/xml.h"
#include "wx/filename.h"
#include "wx/wfstream.h"
#include "wx/zipstrm.h"
#include "wx/dir.h"
#include "global.h"
#include "rDb.h"
#include "logger.h"

#else
#include "rDb.h"
#endif

#include "ZIP.h"
#include "logger.h"

namespace fs = std::filesystem;

static constexpr int numOfDailyBackup = 7;
static void DoTrimWeeklyFolder(const std::string &folderName) {
    // keep only latest 7 zip file
    ShowLog(DB::concat("Trimming weekly backup folder ", folderName));
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
            ShowLog("Removing old file " + *it);
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
            ShowLog(fmt::format("Hourly Backup Master started. {}", GetDBName()));
            wpSQLDatabase toBackup;
            toBackup.Open(GetDBName());
            if (toBackup.BackupTo(folderName + fileName, fnIsStopping) != SQLITE_OK) return false;
            ShowLog(fmt::format("Hourly Backup Master completed. {}", GetDBName()));
        }
        if (fnIsStopping && fnIsStopping()) return false;
        if (!tFileName.empty()) {
            ShowLog(fmt::format("Hourly Backup Transaction started. {}", GetTransactionDBName()));
            wpSQLDatabase toBackup;
            toBackup.Open(GetTransactionDBName());
            if (toBackup.BackupTo(folderName + tFileName, fnIsStopping) != SQLITE_OK) return false;
            ShowLog(fmt::format("Hourly Backup Transaction completed. {}", GetTransactionDBName()));
        }

        if (fnIsStopping && fnIsStopping()) return false;
        {
            ShowLog("Zipping backup started.");
            auto f = folderName + "backup" + GetEpoch() + ".zip";
            Partio::ZipFileWriter zip(f);
            {
                std::ostream* o = zip.Add_File(fileName);
                std::ifstream in(folderName + fileName, std::ios::in | std::ios::binary);
                ShowLog("Zipping " + folderName + fileName);
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
                ShowLog("Zipping " + folderName + tFileName);
                size_t size {50000};
                while (!in.eof()) {
                    std::string buf(size, '\0');
                    in.read(&buf[0], size);
                    o->write(buf.data(), in.gcount());
                }
                o->flush();
                delete o;
            }
            ShowLog("Zipping backup completed.");
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
                // ShowLog("Checking " + *it + "(" + std::to_string(t.time_since_epoch().count()) + ")-> " + FormatDate(t) + " vs " + FormatDate(time_now));
                // ShowLog("Begining of day " + std::to_string(GetBeginOfDay(t).time_since_epoch().count()) + " vs " + std::to_string(GetBeginOfDay(time_now).time_since_epoch().count()));
                if (GetBeginOfDay(t) != GetBeginOfDay(time_now)) {
                    needTrimWeekly = true;
                    std::string newName = folderName + std::string("Weekly") + std::string(1, fs::path::preferred_separator) + fileName;
                    ShowLog("renaming " + *it + " to " + newName);
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
        ShowLog(fmt::format("Backup error: {}", e.what()));
    } catch (wpSQLException& e) {
        ShowLog(fmt::format("Backup sql error: {}", e.message));
    } catch (...) {
        ShowLog("Backup error: unknown!");
    }
    return false;
}
