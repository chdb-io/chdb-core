// Error / status model. (Type-stripping friendly: no `enum`.)
/** Coarse status of a chdb operation. */
export const StatusCode = {
    OK: 'OK',
    ERROR: 'ERROR',
};
/** Error thrown when a chdb query fails. Carries the offending SQL when known. */
export class ChdbError extends Error {
    sql;
    constructor(message, sql) {
        super(message);
        this.name = 'ChdbError';
        this.sql = sql;
    }
}
//# sourceMappingURL=status.js.map