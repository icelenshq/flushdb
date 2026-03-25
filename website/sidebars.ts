import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'index',
    'architecture',
    {
      type: 'category',
      label: 'Storage Engine Internals',
      link: { type: 'doc', id: 'internals/index' },
      items: [
        'internals/composite-key',
        'internals/wal',
        'internals/memtable',
        'internals/sstable',
        'internals/manifest',
        'internals/flush',
        'internals/compaction',
        'internals/cache',
        'internals/s3',
        'internals/data-flow',
      ],
    },
    'api',
    'operations',
  ],
};

export default sidebars;
