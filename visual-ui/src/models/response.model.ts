/**
 * Generic Response wrapper matching backend structure
 * @template T - The type of data returned in the response
 */
export interface Response<T> {
  code: number;
  message: string;
  data: T;
}

/**
 * Response status enum matching backend ResponseStatusEnum
 */
export const ResponseStatus = {
  SUCCESS: 200,
  ERROR: 500,
  BAD_REQUEST: 400,
  UNAUTHORIZED: 401,
  FORBIDDEN: 403,
  NOT_FOUND: 404,
} as const;

export type ResponseStatus = typeof ResponseStatus[keyof typeof ResponseStatus];

/**
 * Helper function to check if response is successful
 */
export const isSuccessResponse = <T>(response: Response<T>): boolean => {
  return response.code === ResponseStatus.SUCCESS;
};
