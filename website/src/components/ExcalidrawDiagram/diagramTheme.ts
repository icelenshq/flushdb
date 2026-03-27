/**
 * Excalidraw 0.18 font family IDs.
 * @see https://docs.excalidraw.com/docs/@excalidraw/excalidraw/api/props/excalidraw-api#updatescene
 */
export const FONT_FAMILY = {
  Excalidraw: 1,
  Nunito: 2,
  ComicShanns: 3,
  LiberationSans: 4,
} as const;

export interface DiagramTheme {
  fontFamily?: number;
  fontSize?: number;
  strokeColor?: string;
  strokeWidth?: number;
  roughness?: number;
}

export const defaultTheme: DiagramTheme = {
  fontFamily: FONT_FAMILY.ComicShanns,
};

type ExcalidrawElement = Record<string, unknown>;

const THEMEABLE_KEYS: ReadonlyArray<keyof DiagramTheme> = [
  'fontFamily',
  'fontSize',
  'strokeColor',
  'strokeWidth',
  'roughness',
];

export function applyTheme(
  elements: ExcalidrawElement[],
  theme: DiagramTheme = defaultTheme,
): ExcalidrawElement[] {
  return elements.map((el) => {
    const patched = {...el};
    for (const key of THEMEABLE_KEYS) {
      if (theme[key] !== undefined && key in el) {
        (patched as Record<string, unknown>)[key] = theme[key];
      }
    }
    return patched;
  });
}
