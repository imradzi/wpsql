#include <filesystem>

#include "rDb.h"

int noOfWaitingIteration = 100;  // per return of SQLITE_BUSY after 100 sec
int secPerSleep = 1;

int waitPerSlice = 1000;  // ms
int noOfTries = 10;

int waitingFunction(void*, int nCall) {
    //std::cout << fmt::format("waiting for lock release : {}", nCall) << std::endl;
    if (nCall >= noOfTries) {
        return 0;
    }
    return 1;
}

// ensure lock working use BEGIN and COMMIT
void TestLock() {
    std::cout << "TestLock: starts" << std::endl;
    try {
        std::filesystem::remove("testLock.db");
        DB::SQLiteBase testDB("testLock.db");
        testDB.Open();
        testDB.GetSession().ExecuteUpdate("create table test(id int primary key, name text)");

        std::thread thr([&]() {
            try {
                std::this_thread::yield();
                std::this_thread::sleep_for(1s);
                DB::SQLiteBase dbInner("testLock.db");
                dbInner.Open();
                std::cout << "TestLock:Inner. begin transaction." << std::endl;
                dbInner.GetSession().Begin();
                dbInner.GetSession().ExecuteUpdate("insert into test(name) values('from thread')");
                dbInner.GetSession().Commit();
                std::cout << "TestLock:Inner. commit transaction." << std::endl;
                dbInner.Close();
            } catch (wpSQLException& e) {
                std::cout << fmt::format("TestLock:Inner sql exception: {}", e.message) << std::endl;
            } catch (std::exception& e) {
                std::cout << fmt::format("TestLock:Inner std exception: {}", e.what()) << std::endl;
            } catch (...) {
                std::cout << "TestLock:Inner unknown exception" << std::endl;
            }
        });

        std::cout << "TestLock:main thread insert." << std::endl;
        testDB.GetSession().Begin();
        std::this_thread::yield();
        std::this_thread::sleep_for(60s);
        testDB.GetSession().ExecuteUpdate("insert into test(name) values('main thread')");
        testDB.GetSession().Commit();
        std::cout << "TestLock:main commit." << std::endl;
        testDB.Close();
        thr.join();

    } catch (wpSQLException& e) {
        std::cout << fmt::format("TestLock:main sql exception: {}", e.message) << std::endl;
    } catch (std::exception& e) {
        std::cout << fmt::format("TestLock:main std exception: {}", e.what()) << std::endl;
    } catch (...) {
        std::cout << "TestLock:main unknown exception" << std::endl;
    }
    std::cout << "TestLock: DONE" << std::endl;
}
