import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'flushdb',
  tagline: 'A distributed key-value database using S3 as source of truth',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://flushdb.com',
  baseUrl: '/',

  organizationName: 'icelenshq',
  projectName: 'flushdb',

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: '/',
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/icelenshq/flushdb/edit/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'flushdb',
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://github.com/icelenshq/flushdb',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            { label: 'Architecture', to: '/architecture' },
            { label: 'API Reference', to: '/api' },
            { label: 'Operations', to: '/operations' },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/icelenshq/flushdb',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} icelens`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['protobuf', 'rust', 'toml', 'bash', 'json'],
    },
  } satisfies Preset.ThemeConfig,

  plugins: [
    function excalidrawWebpackPlugin() {
      return {
        name: 'excalidraw-webpack-plugin',
        configureWebpack() {
          return {
            module: {
              rules: [
                {
                  test: /\.m?js$/,
                  resolve: {
                    fullySpecified: false,
                  },
                },
              ],
            },
          };
        },
      };
    },
  ],
};

export default config;
