import { z } from 'zod';

export const FavoriteMealSchema = z.object({
  value: z.string().min(1).max(500),
});

export type FavoriteMealInput = z.infer<typeof FavoriteMealSchema>;
