/** Coarse status of a chdb operation. */
export declare const StatusCode: {
    readonly OK: "OK";
    readonly ERROR: "ERROR";
};
export type StatusCode = (typeof StatusCode)[keyof typeof StatusCode];
/** Error thrown when a chdb query fails. Carries the offending SQL when known. */
export declare class ChdbError extends Error {
    readonly sql?: string;
    constructor(message: string, sql?: string);
}
//# sourceMappingURL=status.d.ts.map