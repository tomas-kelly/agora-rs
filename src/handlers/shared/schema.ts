import { z } from 'zod';

export const PreferenceInputSchema = z.object({
  food_name: z.string().min(1).max(200),
  category: z.string().max(100).optional(),
  rating: z.number().int().min(1).max(5).optional(),
  tags: z.array(z.string()).max(10).optional(),
  notes: z.string().max(1000).optional(),
});

export type PreferenceInput = z.infer<typeof PreferenceInputSchema>;
