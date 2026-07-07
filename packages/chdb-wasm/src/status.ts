// Error / status model. (Type-stripping friendly: no `enum`.)

/** Coarse status of a chdb operation. */
export const StatusCode = {
  OK: 'OK',
  ERROR: 'ERROR',
} as const;
export type StatusCode = (typeof StatusCode)[keyof typeof StatusCode];

/** Error thrown when a chdb query fails. Carries the offending SQL when known. */
export class ChdbError extends Error {
  readonly sql?: string;
  constructor(message: string, sql?: string) {
    super(message);
    this.name = 'ChdbError';
    this.sql = sql;
  }
}
