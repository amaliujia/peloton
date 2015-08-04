/*-------------------------------------------------------------------------
 *
 * logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/logging/logdefs.h"
#include "backend/logging/logproxy.h"
#include "backend/logging/logrecord.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Logger 
//===--------------------------------------------------------------------===//

class Logger{

  public:
    Logger() = delete;

    Logger(LoggerId logger_id, LogProxy *proxy) 
    : logger_id(logger_id), proxy(proxy) {};
    
    void logging_MainLoop(void);

    void log(LogRecord record);

  private:
    LoggerId logger_id = INVALID_LOGGER_ID;

    LogProxy *proxy;
};

}  // namespace logging
}  // namespace peloton
