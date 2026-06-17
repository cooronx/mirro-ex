import { createApp } from 'vue';
import TDesign from 'tdesign-vue-next';
import 'tdesign-vue-next/es/style/index.css';

import App from './App.vue';
import './style.css';

createApp(App).use(TDesign).mount('#app');
