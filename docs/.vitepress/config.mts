import { defineConfig } from 'vitepress'

export default defineConfig({
    srcDir: './src',
    title: 'FrankenPHP Tiered Cache',
    description: 'Документация FrankenPHP Tiered Cache',
    base: '/frankenphp-tiered-cache/',
    lang: 'ru-RU',
    themeConfig: {
        nav: [
            { text: 'Главная', link: '/' },
            { text: 'Архитектура', link: '/architecture' },
            { text: 'PHP API', link: '/php-api' },
            { text: 'Развёртывание', link: '/deployment' }
        ],
        sidebar: [
            {
                text: 'Использование',
                items: [
                    { text: 'Архитектура', link: '/architecture' },
                    { text: 'Конфигурация', link: '/configuration' },
                    { text: 'PHP API', link: '/php-api' },
                    { text: 'Развёртывание', link: '/deployment' }
                ]
            },
            {
                text: 'Эксплуатация',
                items: [
                    { text: 'Наблюдаемость', link: '/observability' },
                    { text: 'Память L1', link: '/memory-cache' }
                ]
            },
            {
                text: 'Разработка',
                items: [
                    { text: 'Локальная разработка', link: '/development' },
                    { text: 'Бенчмарки', link: '/benchmarks' }
                ]
            }
        ],
        search: { provider: 'local' },
        socialLinks: [
            { icon: 'github', link: 'https://github.com/b1tc0re/frankenphp-tiered-cache' }
        ]
    }
})
