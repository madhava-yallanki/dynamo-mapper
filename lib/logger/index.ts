import pino from 'pino';

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  serializers: { error: pino.stdSerializers.err },
});

type LoggerArgs = {
  module: string;
};

export class Logger {
  readonly childLogger: pino.Logger;

  constructor({ module }: LoggerArgs) {
    this.childLogger = logger.child({ module });
  }

  debug: pino.LogFn = (obj: unknown, msg?: string) => {
    this.childLogger.debug(obj, msg);
  };

  info: pino.LogFn = (obj: unknown, msg?: string) => {
    this.childLogger.info(obj, msg);
  };

  warn: pino.LogFn = (obj: unknown, msg?: string) => {
    this.childLogger.warn(obj, msg);
  };

  error: pino.LogFn = (obj: unknown, msg?: string) => {
    this.childLogger.error(obj, msg);
  };
}
